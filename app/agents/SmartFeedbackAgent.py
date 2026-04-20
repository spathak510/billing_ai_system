from __future__ import annotations

import re
import time
import pandas as pd
from openai import OpenAI
from pathlib import Path

from app.agents.mail_reader_agent import MailReaderAgent
from app.services.base import EmailMessage
from app.ai.clasification_rules import rules


class SmartFeedbackAgent:
    """
    Smart Billing Email Agent

    Features:
    - Read unread emails
    - Extract order numbers
    - Search order in Excel
    - Save matched rows
    - Scenario-based replies
    """

    def __init__(self, api_key: str=None, mail_agent=None, source_folders: list[str] = None, output_folder: str = None):
        # self.client = OpenAI(api_key=api_key)
        self.mail_agent = mail_agent or MailReaderAgent()

        # source_folders: list of paths to feedback/Crop and feedback/NonCrop folders containing Excel files
        self.source_folders_corp = Path("feedback/Crop/")
        self.source_folders_noncorp = Path("feedback/NonCrop/")
        self.output_folder = Path("feedback/feedback_data")

        import os
        os.makedirs(self.output_folder, exist_ok=True)
        
    # Email Classification (Concern / Feedback / Neutral)
    def _classify_email(self, subject: str, body: str) -> str:
        text = f"{subject} {body}".lower()

        # Priority → Concern first
        result = "neutral"
        if any(keyword in text for keyword in rules[0]["keywords"]):
            result = rules[0]["category"]
        if any(keyword in text for keyword in rules[1]["keywords"]):
            result = rules[1]["category"]
        
        return result  

   
    # Extract Order Numbers
    def _extract_reference_numbers(self, subject: str, body: str) -> dict:
        text = f"{subject}\n{body}"
        found = set()
        classify_email_response = self._classify_email(subject=subject, body=body)
        if classify_email_response != "neutral":
            # Clean text
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text).strip()

            patterns = [
                # Order
                r"order\s*(number|no\.?|id|#)?\s*[:\-#]?\s*(\d{6,15})",

                # Incident
                r"\b(inc\d{5,15})\b",
                r"incident\s*(number|no\.?|#)?\s*[:\-#]?\s*(\d{5,15})",

                # Ticket
                r"\b(tkt\d{5,15})\b",
                r"ticket\s*(number|no\.?|#)?\s*[:\-#]?\s*(\d{5,15})"
            ]

            # found = set()

            for pattern in patterns:
                matches = re.findall(pattern, text, flags=re.IGNORECASE)

                for match in matches:
                    if isinstance(match, tuple):
                        value = match[-1]
                    else:
                        value = match

                    found.add(value.upper())

            return {
                "status": bool(found),
                "order_number": list(found)
            }
        else:
            return {
                "status": bool(found),
                "classification": "neutral",
                "order_number": []
            }
    
    # Search Orders in Excel + Save Matches
    def process_orders(self, order_numbers: list[str], email: EmailMessage) -> bool:
        import os
        
        result = {
                "status": False,
                "matched": []    
        }

        try:
            # Collect all Excel/CSV file paths from Corp and Noncorp
            comparison_file_paths = [
                str(p) for p in self.source_folders_corp.glob("*.xlsx")
            ] + [
                str(p) for p in self.source_folders_corp.glob("*.csv")
            ] + [
                str(p) for p in self.source_folders_noncorp.glob("*.xlsx")
            ] + [
                str(p) for p in self.source_folders_noncorp.glob("*.csv")
            ]

            for file_path in comparison_file_paths:
                if file_path.endswith(".xlsx") or file_path.endswith(".xls") or file_path.endswith(".csv"):
                    df = pd.read_excel(file_path, dtype=str).fillna("")
                    for order in order_numbers:
                        temp = df[
                            df.apply(
                                lambda row: row.astype(str)
                                .str.contains(order, case=False, na=False)
                                .any(),
                                axis=1
                            )
                        ].copy()
                        if not temp.empty:
                            temp["source_file"] = file_path  # Track which file matched
                            result["matched"].append(temp)

            if not result["matched"]:
                return False

            matched_rows = pd.concat(result["matched"], ignore_index=True)
            matched_rows.drop_duplicates(inplace=True)

            matched_rows["mail_id"] = getattr(email, "sender", "")
            matched_rows["sender_name"] = getattr(email, "sender_name", "")
            matched_rows["email_subject"] = email.subject

            # Save output to feedback/feedback_data with a unique filename
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.output_folder, f"matched_orders_{timestamp}.xlsx")
            matched_rows.to_excel(output_file, index=False)
            print(f"Matched rows saved to {output_file}.")

            result["status"] = True
            return result

        except Exception as e:
            print("Excel Error:", e)
            return False
   
    # Reply - No Order Mentioned
    def no_order_found_reply(self, sender_name: str) -> str:
        sender_name = sender_name.split(",")[0].strip()

        return f"""
        <p><b>Dear {sender_name},</b></p>

        <p>Thank you for your email.</p>

        <p>We were unable to identify any Order Number in your request.</p>

        <p>Kindly provide the relevant Order Number so that we can review the billing details and assist you further.</p>

        <p>Once received, we will check and update you accordingly.</p>

        <p>Best Regards,<br>
        <b>Billing AI Support Team</b></p>
        """

   
    # Reply - Order Mentioned But Not Found
   
    def order_number_not_match_reply(self, sender_name: str, orders: list[str]) -> str:
        sender_name = sender_name.split(",")[0].strip()
        order_text = ", ".join(orders)

        return f"""
        <p><b>Dear {sender_name},</b></p>

        <p>Thank you for your email.</p>

        <p>We reviewed your request, however we were unable to locate the provided Order Number(s): <b>{order_text}</b> in our records.</p>

        <p>Kindly recheck the Order Number and share the correct details so that we can investigate further.</p>

        <p>Once received, we will review and update you at the earliest.</p>

        <p>Best Regards,<br>
        <b>Billing AI Support Team</b></p>
        """

   
    # Reply - Order Found
   
    def generate_reply(self, email: EmailMessage, sender_name: str) -> str:
        clean_name = sender_name.split(",")[0].strip()

        prompt = f"""
        You are a professional billing support analyst.

        Write a polished business email reply in VALID HTML only.

        STRICT RULES:
        - Do NOT write the word html
        - Start exactly with:
        <p><b>Dear {clean_name},</b></p>

        - Use proper paragraphs with <p> tags
        - Use professional corporate tone
        - Mention that we are reviewing the billing query
        - Mention validation is in progress
        - Reassure user we will update shortly
        - Do not repeat subject line
        - End exactly with:

        <p>Best Regards,<br>
        <b>Billing AI Support Team</b></p>

        Return ONLY HTML.

        Email Subject:
        {email.subject}

        Email Body:
        {email.body}
        """

        # response = self.client.chat.completions.create(
        #     model="gpt-4o-mini",
        #     messages=[{"role": "user", "content": prompt}],
        #     temperature=0.2
        # )

        # return response.choices[0].message.content.strip()
        return True

  
    # Main Process Inbox

    def process_inbox(self, limit: int = 25) -> list[str]:
        emails = self.mail_agent.fetch_unread(limit=limit)
        replied = []

        for email in emails:
            try:
                sender_name = getattr(email, "sender_name", "Team") or "Team"

                print("Processing:", email.subject)

                order_numbers = self.extract_order_numbers(
                    email.subject,
                    email.body
                )

                # CASE 1: No Order Mentioned
                if not order_numbers:
                    print("No order number in mail")
                    reply_body = self.generate_no_order_reply(sender_name)

                else:
                    print("Orders Found:", order_numbers)

                    found = self.process_orders(order_numbers, email)

                    # CASE 2: Order Found in Excel
                    if found:
                        reply_body = self.generate_reply(email, sender_name)

                    # CASE 3: Order Not Found
                    else:
                        reply_body = self.generate_order_not_found_reply(
                            sender_name,
                            order_numbers
                        )

                self.mail_agent.reply_email(
                    email_id=email.id,
                    body=reply_body
                )

                replied.append(email.id)
                print("Reply sent.")

            except Exception as e:
                print("Error:", e)

        return replied


    # Continuous Loop
   
    def monitor_loop(self, interval: int = 60):
        while True:
            try:
                self.process_inbox()
            except Exception as e:
                print("Monitor Error:", e)

            time.sleep(interval)


    def search_order_number(self,email: EmailMessage) -> dict:
        try:
            order_numbers_response = self._extract_reference_numbers(subject=email.subject, body=email.body)
            if order_numbers_response["status"]:
                process_orders_response = self.process_orders(order_numbers_response["order_number"], email)
                if process_orders_response['status'] == False:
                    reply_body = self.order_number_not_match_reply(sender_name=email.sender_name)
                    self.mail_agent.reply_email(
                    email_id=email.id,
                    body=reply_body
                )
                    return {
                        "status": "Not_Matched",
                        "order_numbers": order_numbers_response["order_number"],
                        "message": "Order numbers found but not matched in records."
                    }
                else:
                    return {
                        "status": "success",
                        "order_numbers": order_numbers_response["order_number"],
                        "message": "Order numbers found and matched in records."
                    }
            else:
                reply_body = self.no_order_found_reply(sender_name=email.sender_name)
                self.mail_agent.reply_email(
                    email_id=email.id,
                    body=reply_body
                )
                return {
                    "status": "No_Order_Found",
                    "order_numbers": order_numbers_response["order_number"],
                    "message": "No order numbers found in the email."
                }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }