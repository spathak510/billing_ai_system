from __future__ import annotations

import re
import time
import pandas as pd
from openai import OpenAI

from app.agents.mail_reader_agent import MailReaderAgent
from app.services.base import EmailMessage


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

    def __init__(self, api_key: str, mail_agent=None):
        self.client = OpenAI(api_key=api_key)
        self.mail_agent = mail_agent or MailReaderAgent()

        self.source_excel = "output/filtered_non_zero_data.xlsx"
        self.output_excel = "output/matched_orders.xlsx"

   
    # Extract Order Numbers
   
    def extract_order_numbers(self, subject: str, body: str) -> list[str]:
        text = f"{subject}\n{body}"

        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)

        patterns = [
            r"order\s*number\s*[:\-#]?\s*(\d{6,15})",
            r"order\s*no\.?\s*[:\-#]?\s*(\d{6,15})",
            r"order\s*id\s*[:\-#]?\s*(\d{6,15})",
            r"order\s*#\s*(\d{6,15})",
            r"\b\d{6,15}\b"
        ]

        found = set()

        for p in patterns:
            matches = re.findall(p, text, flags=re.IGNORECASE)
            found.update(matches)

        return list(found)
    
    # Search Orders in Excel + Save Matches

    def process_orders(self, order_numbers: list[str], email: EmailMessage) -> bool:
        try:
            df = pd.read_excel(self.source_excel, dtype=str).fillna("")
            matched = []

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
                    matched.append(temp)

            if not matched:
                return False

            matched_rows = pd.concat(matched, ignore_index=True)
            matched_rows.drop_duplicates(inplace=True)

            matched_rows["mail_id"] = getattr(email, "sender", "")
            matched_rows["sender_name"] = getattr(email, "sender_name", "")
            matched_rows["email_subject"] = email.subject

            matched_rows.to_excel(self.output_excel, index=False)
            print("Matched rows saved.")

            return True

        except Exception as e:
            print("Excel Error:", e)
            return False
   
    # Reply - No Order Mentioned
    def generate_no_order_reply(self, sender_name: str) -> str:
        clean_name = sender_name.split(",")[0].strip()

        return f"""
<p><b>Dear {clean_name},</b></p>

<p>Thank you for your email.</p>

<p>We were unable to identify any Order Number in your request.</p>

<p>Kindly provide the relevant Order Number so that we can review the billing details and assist you further.</p>

<p>Once received, we will check and update you accordingly.</p>

<p>Best Regards,<br>
<b>Billing AI Support Team</b></p>
"""

   
    # Reply - Order Mentioned But Not Found
   
    def generate_order_not_found_reply(self, sender_name: str, orders: list[str]) -> str:
        clean_name = sender_name.split(",")[0].strip()
        order_text = ", ".join(orders)

        return f"""
<p><b>Dear {clean_name},</b></p>

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

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )

        return response.choices[0].message.content.strip()

  
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