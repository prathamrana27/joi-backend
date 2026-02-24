# tools/whatsapp_automation.py

import pyautogui
import time
import webbrowser
from typing import Dict

def send_whatsapp_message(args: Dict[str, str]) -> str:
    """
    Sends a WhatsApp message to a contact using their name via WhatsApp Web.
    This is a more robust version that uses GUI automation to find the contact.
    The user must be logged into WhatsApp Web, and the browser window
    should be in the foreground for this to work reliably.

    Args:
        args (Dict[str, str]): Requires:
            - contact: the name of the contact as saved in WhatsApp
            - message: the message to send

    Returns:
        str: Status message of the operation.
    """
    contact = args.get("contact")
    message = args.get("message")

    if not contact or not message:
        return "Error: Missing 'contact' or 'message' in arguments."

    try:
        # Step 1: Open WhatsApp Web in a new browser tab.
        webbrowser.open("https://web.whatsapp.com/")
        # Wait for the page to load completely. This time may need adjustment.
        time.sleep(12)

        # Step 2: Use the browser's "Find" feature to locate the contact. This is more reliable.
        # This brings focus to the browser window and searches the page.
        pyautogui.hotkey("ctrl", "f")
        time.sleep(1)
        pyautogui.write(contact, interval=0.1)
        time.sleep(2) # Wait for browser to highlight the contact.

        # Step 3: Close the find dialog and select the highlighted contact.
        # Pressing Escape closes the find bar, usually leaving focus on the found item.
        pyautogui.press("esc")
        time.sleep(1)
        # Pressing Enter will now "click" the focused contact chat.
        pyautogui.press("enter")
        time.sleep(2) # Wait for the chat to open on the right.

        # Step 4: Type and send the message.
        # The cursor should now be in the message input box.
        pyautogui.typewrite(message, interval=0.05)
        pyautogui.press("enter")
        time.sleep(1)

        return f"Successfully attempted to send message to '{contact}'."

    except Exception as e:
        return f"Error sending WhatsApp message to {contact}: {str(e)}"