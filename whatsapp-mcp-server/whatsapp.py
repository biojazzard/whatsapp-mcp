import sqlite3
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Tuple
from dateutil.parser import parse
import os.path
import requests
import json

MESSAGES_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'whatsapp-bridge', 'store', 'messages.db')
WHATSAPP_API_BASE_URL = "http://localhost:8080/api"

@dataclass
class Message:
    timestamp: datetime
    sender: str
    content: str
    is_from_me: bool
    chat_jid: str
    id: str
    chat_name: Optional[str] = None

@dataclass
class Chat:
    jid: str
    name: Optional[str]
    last_message_time: Optional[datetime]
    last_message: Optional[str] = None
    last_sender: Optional[str] = None
    last_is_from_me: Optional[bool] = None

    @property
    def is_group(self) -> bool:
        """Determine if chat is a group based on JID pattern."""
        return self.jid.endswith("@g.us")

@dataclass
class Contact:
    phone_number: str
    name: Optional[str]
    jid: str

@dataclass
class MessageContext:
    message: Message
    before: List[Message]
    after: List[Message]

def print_message(message: Message, show_chat_info: bool = True) -> None:
    """Print a single message with consistent formatting."""
    direction = "→" if message.is_from_me else "←"
    
    if show_chat_info and message.chat_name:
        print(f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] {direction} Chat: {message.chat_name} ({message.chat_jid})")
    else:
        print(f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] {direction}")
        
    print(f"From: {'Me' if message.is_from_me else message.sender}")
    print(f"Message: {message.content}")
    print("-" * 100)

def print_messages_list(messages: List[Message], title: str = "", show_chat_info: bool = True) -> None:
    """Print a list of messages with a title and consistent formatting."""
    if not messages:
        print("No messages to display.")
        return
        
    if title:
        print(f"\n{title}")
        print("-" * 100)
    
    for message in messages:
        print_message(message, show_chat_info)

def print_chat(chat: Chat) -> None:
    """Print a single chat with consistent formatting."""
    print(f"Chat: {chat.name} ({chat.jid})")
    if chat.last_message_time:
        print(f"Last active: {chat.last_message_time:%Y-%m-%d %H:%M:%S}")
        direction = "→" if chat.last_is_from_me else "←"
        sender = "Me" if chat.last_is_from_me else chat.last_sender
        print(f"Last message: {direction} {sender}: {chat.last_message}")
    print("-" * 100)

def print_chats_list(chats: List[Chat], title: str = "") -> None:
    """Print a list of chats with a title and consistent formatting."""
    if not chats:
        print("No chats to display.")
        return
        
    if title:
        print(f"\n{title}")
        print("-" * 100)
    
    for chat in chats:
        print_chat(chat)

def print_paginated_messages(messages: List[Message], page: int, total_pages: int, chat_name: str) -> None:
    """Print a paginated list of messages with navigation hints."""
    print(f"\nMessages for chat: {chat_name}")
    print(f"Page {page} of {total_pages}")
    print("-" * 100)
    
    print_messages_list(messages, show_chat_info=False)
    
    # Print pagination info
    if page > 1:
        print(f"Use page={page-1} to see newer messages")
    if page < total_pages:
        print(f"Use page={page+1} to see older messages")

"""
CREATE TABLE messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TEXT,
			is_from_me BOOLEAN,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		)

CREATE TABLE chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		)
"""

def print_recent_messages(limit=10) -> List[Message]:
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        # Query recent messages with chat info
        query = """
        SELECT 
            strftime('%Y-%m-%d %H:%M:%S', m.timestamp) as formatted_timestamp,
            m.sender,
            c.name,
            m.content,
            m.is_from_me,
            c.jid,
            m.id
        FROM messages m
        JOIN chats c ON m.chat_jid = c.jid
        ORDER BY m.timestamp DESC
        LIMIT ?
        """
        
        cursor.execute(query, (limit,))
        messages = cursor.fetchall()
        
        if not messages:
            print("No messages found in the database.")
            return []
            
        result = []
        
        # Convert to Message objects
        for msg in messages:
            message = Message(
                timestamp=parse(msg[0]),
                sender=msg[1],
                chat_name=msg[2] or "Unknown Chat",
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6]
            )
            result.append(message)
        
        # Print messages using helper function
        print_messages_list(result, title=f"Last {limit} messages:")
        return result
            
    except sqlite3.Error as e:
        print(f"Error accessing database: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def list_messages(
    date_range: Optional[Tuple[datetime, datetime]] = None,
    sender_phone_number: Optional[str] = None,
    chat_jid: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1
) -> List[Message]:
    """Get messages matching the specified criteria with optional context."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        # Use row factory for easier dictionary access if needed later, though not strictly necessary here
        # conn.row_factory = sqlite3.Row 
        cursor = conn.cursor()
        
        # Build base query
        # Select columns explicitly for clarity
        query_parts = [
            "SELECT m.timestamp, m.sender, c.name, m.content, m.is_from_me, m.chat_jid, m.id"
            " FROM messages m"
            " JOIN chats c ON m.chat_jid = c.jid"
        ]
        where_clauses = []
        params = []
        
        # Add filters
        if date_range:
            # --- MODIFICATION START ---
            # Ensure the datetime objects are formatted correctly for SQLite comparison
            # It's also good practice to handle potential timezone awareness
            start_dt = date_range[0]
            end_dt = date_range[1]
            
            # Optional: If you standardize on UTC storage (see Go changes below),
            # ensure query dates are also UTC or handled appropriately.
            # if start_dt.tzinfo:
            #     start_dt = start_dt.astimezone(timezone.utc)
            # if end_dt.tzinfo:
            #     end_dt = end_dt.astimezone(timezone.utc)

            # Format as 'YYYY-MM-DD HH:MM:SS' which SQLite compares reliably
            where_clauses.append("m.timestamp BETWEEN ? AND ?")
            params.extend([
                start_dt.strftime('%Y-%m-%d %H:%M:%S'), 
                end_dt.strftime('%Y-%m-%d %H:%M:%S')
            ])
            # --- MODIFICATION END ---
            
        if sender_phone_number:
            # Assuming sender is stored just as the user part of JID in messages table
            where_clauses.append("m.sender = ?") 
            params.append(sender_phone_number) # Ensure this matches how sender is stored
            
        if chat_jid:
            where_clauses.append("m.chat_jid = ?")
            params.append(chat_jid)
            
        if query:
            where_clauses.append("LOWER(m.content) LIKE LOWER(?)")
            params.append(f"%{query}%")
            
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add pagination
        offset = page * limit
        query_parts.append("ORDER BY m.timestamp DESC") # Use alias 'm'
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])
        
        # Debug: Print the final query and params
        # print("Executing SQL:", " ".join(query_parts))
        # print("With parameters:", tuple(params))

        cursor.execute(" ".join(query_parts), tuple(params))
        messages_data = cursor.fetchall()
        
        result = []
        message_ids_for_context = [] # Collect IDs if context is needed

        for msg_data in messages_data:
            try:
                message = Message(
                    # Use dateutil.parser for flexibility in reading various stored formats
                    timestamp=parse(msg_data[0]), 
                    sender=msg_data[1],
                    chat_name=msg_data[2], # Already joined from chats table
                    content=msg_data[3],
                    is_from_me=bool(msg_data[4]), # Ensure boolean conversion
                    chat_jid=msg_data[5], # Use the specific chat_jid from the message row
                    id=msg_data[6]
                )
                result.append(message)
                if include_context:
                    message_ids_for_context.append(message.id)
            except Exception as parse_err:
                 print(f"Error parsing message data row {msg_data}: {parse_err}")
                 continue # Skip problematic rows


        if include_context and message_ids_for_context:
            # Note: Fetching context individually can be inefficient (N+1 problem).
            # A more optimized approach might involve a single query fetching all relevant messages.
            # However, sticking to the current structure for now:
            messages_with_context_map = {} # Use map to avoid duplicates and maintain order
            all_context_messages = []

            for msg_id in message_ids_for_context:
                try:
                    context = get_message_context(msg_id, context_before, context_after)
                    
                    # Add messages to map using timestamp as key to sort later
                    for m in context.before:
                        messages_with_context_map[m.timestamp] = m
                    messages_with_context_map[context.message.timestamp] = context.message
                    for m in context.after:
                         messages_with_context_map[m.timestamp] = m

                except ValueError:
                     # Handle case where message for context might not be found (e.g., deleted)
                     print(f"Warning: Could not get context for message ID {msg_id}, it might not exist.")
                     # Try to find the original message in the 'result' list and add it
                     original_msg = next((m for m in result if m.id == msg_id), None)
                     if original_msg:
                         messages_with_context_map[original_msg.timestamp] = original_msg

            # Sort messages by timestamp and return as a list
            all_context_messages = [messages_with_context_map[ts] for ts in sorted(messages_with_context_map)]
            return all_context_messages
            
        return result
        
    except sqlite3.Error as e:
        print(f"Database error in list_messages: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error in list_messages: {e}")
        # Reraise or handle more gracefully depending on needs
        raise # Reraise to see the full traceback in MCP
    finally:
        if 'conn' in locals():
            conn.close()


def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5
) -> MessageContext:
    """Get context around a specific message."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        # Get the target message first
        cursor.execute("""
            SELECT m.timestamp, m.sender, c.name, m.content, m.is_from_me, m.chat_jid, m.id
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.id = ?
        """, (message_id,))
        msg_data = cursor.fetchone()
        
        if not msg_data:
            raise ValueError(f"Message with ID {message_id} not found")
            
        target_timestamp = msg_data[0]  # Now in ISO format
        target_chat_jid = msg_data[5]

        target_message = Message(
            timestamp=parse(target_timestamp),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=bool(msg_data[4]),
            chat_jid=target_chat_jid,
            id=msg_data[6]
        )
        
        # Get messages before
        cursor.execute("""
            SELECT m.timestamp, m.sender, c.name, m.content, m.is_from_me, m.chat_jid, m.id
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.chat_jid = ? AND m.timestamp < ?
            ORDER BY m.timestamp DESC
            LIMIT ?
        """, (target_chat_jid, target_timestamp, before))
        
        before_messages_data = cursor.fetchall()
        before_messages = []
        for msg in reversed(before_messages_data): # Reverse to get chronological order
            before_messages.append(Message(
                timestamp=parse(msg[0]), sender=msg[1], chat_name=msg[2],
                content=msg[3], is_from_me=bool(msg[4]), chat_jid=msg[5], id=msg[6]
            ))
        
        # Get messages after
        cursor.execute("""
            SELECT m.timestamp, m.sender, c.name, m.content, m.is_from_me, m.chat_jid, m.id
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.chat_jid = ? AND m.timestamp > ?
            ORDER BY m.timestamp ASC
            LIMIT ?
        """, (target_chat_jid, target_timestamp, after))
        
        after_messages_data = cursor.fetchall()
        after_messages = []
        for msg in after_messages_data:
             after_messages.append(Message(
                timestamp=parse(msg[0]), sender=msg[1], chat_name=msg[2],
                content=msg[3], is_from_me=bool(msg[4]), chat_jid=msg[5], id=msg[6]
            ))
        
        return MessageContext(
            message=target_message,
            before=before_messages,
            after=after_messages
        )
        
    except sqlite3.Error as e:
        print(f"Database error in get_message_context: {e}")
        raise
    except ValueError as e: # Catch specific "not found" error
        print(f"Value error in get_message_context: {e}")
        raise
    except Exception as e:
         print(f"Unexpected error in get_message_context: {e}")
         raise
    finally:
        if 'conn' in locals():
            conn.close()



def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active"
) -> List[Chat]:
    """Get chats matching the specified criteria."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        # Build base query
        query_parts = ["""
            SELECT 
                chats.jid,
                chats.name,
                chats.last_message_time,
                messages.content as last_message,
                messages.sender as last_sender,
                messages.is_from_me as last_is_from_me
            FROM chats
        """]
        
        if include_last_message:
            query_parts.append("""
                LEFT JOIN messages ON chats.jid = messages.chat_jid 
                AND chats.last_message_time = messages.timestamp
            """)
            
        where_clauses = []
        params = []
        
        if query:
            where_clauses.append("(LOWER(chats.name) LIKE LOWER(?) OR chats.jid LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])
            
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add sorting
        order_by = "chats.last_message_time DESC" if sort_by == "last_active" else "chats.name"
        query_parts.append(f"ORDER BY {order_by}")
        
        # Add pagination
        offset = (page ) * limit
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])
        
        cursor.execute(" ".join(query_parts), tuple(params))
        chats = cursor.fetchall()
        
        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=parse(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5]
            )
            result.append(chat)
            
        return result
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def search_contacts(query: str) -> List[Contact]:
    """Search contacts by name or phone number."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        # Split query into characters to support partial matching
        search_pattern = '%' +query + '%'
        
        cursor.execute("""
            SELECT DISTINCT 
                jid,
                name
            FROM chats
            WHERE 
                (LOWER(name) LIKE LOWER(?) OR LOWER(jid) LIKE LOWER(?))
                AND jid NOT LIKE '%@g.us'
            ORDER BY name, jid
            LIMIT 50
        """, (search_pattern, search_pattern))
        
        contacts = cursor.fetchall()
        
        result = []
        for contact_data in contacts:
            contact = Contact(
                phone_number=contact_data[0].split('@')[0],
                name=contact_data[1],
                jid=contact_data[0]
            )
            result.append(contact)
            
        return result
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> List[Chat]:
    """Get all chats involving the contact.
    
    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT
                c.jid,
                c.name,
                strftime('%Y-%m-%d %H:%M:%S', c.last_message_time) as formatted_last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            JOIN messages m ON c.jid = m.chat_jid
            WHERE m.sender = ? OR c.jid = ?
            ORDER BY c.last_message_time DESC
            LIMIT ? OFFSET ?
        """, (jid, jid, limit, page * limit))
        
        chats = cursor.fetchall()
        
        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=parse(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5]
            )
            result.append(chat)
            
        return result
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_last_interaction(jid: str) -> Optional[Message]:
    """Get most recent message involving the contact."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                strftime('%Y-%m-%d %H:%M:%S', m.timestamp) as formatted_timestamp,
                m.sender,
                c.name,
                m.content,
                m.is_from_me,
                c.jid,
                m.id
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.sender = ? OR c.jid = ?
            ORDER BY m.timestamp DESC
            LIMIT 1
        """, (jid, jid))
        
        msg_data = cursor.fetchone()
        
        if not msg_data:
            return None
            
        return Message(
            timestamp=parse(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6]
        )
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_chat(chat_jid: str, include_last_message: bool = True) -> Optional[Chat]:
    """Get chat metadata by JID."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        query = """
            SELECT 
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
        """
        
        if include_last_message:
            query += """
                LEFT JOIN messages m ON c.jid = m.chat_jid 
                AND c.last_message_time = m.timestamp
            """
            
        query += " WHERE c.jid = ?"
        
        cursor.execute(query, (chat_jid,))
        chat_data = cursor.fetchone()
        
        if not chat_data:
            return None
            
        return Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=parse(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5]
        )
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_direct_chat_by_contact(sender_phone_number: str) -> Optional[Chat]:
    """Get chat metadata by sender phone number."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                c.jid,
                c.name,
                strftime('%Y-%m-%d %H:%M:%S', c.last_message_time) as formatted_last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            LEFT JOIN messages m ON c.jid = m.chat_jid 
                AND strftime('%Y-%m-%d %H:%M:%S', c.last_message_time) = strftime('%Y-%m-%d %H:%M:%S', m.timestamp)
            WHERE c.jid LIKE ? AND c.jid NOT LIKE '%@g.us'
            LIMIT 1
        """, (f"%{sender_phone_number}%",))
        
        chat_data = cursor.fetchone()
        
        if not chat_data:
            return None
            
        return Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=parse(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5]
        )
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def send_message(phone_number: str, message: str) -> Tuple[bool, str]:
    """Send a WhatsApp message to the specified phone number.
    
    Args:
        phone_number (str): The recipient's phone number, with country code but no + or other symbols
        message (str): The message text to send
        
    Returns:
        Tuple[bool, str]: A tuple containing success status and a status message
    """
    try:
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "phone": phone_number,
            "message": message
        }
        
        response = requests.post(url, json=payload)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"
