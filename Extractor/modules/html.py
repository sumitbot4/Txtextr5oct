import telebot
import os
import re
import random
import time
import asyncio
from telebot.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import threading
from Extractor import app
from collections import defaultdict
import unicodedata
import string
from pyrogram.errors import FloodWait, RPCError
from pyrogram import filters, Client
from flask import Flask
from telebot.apihelper import ApiTelegramException
from pymongo import MongoClient

# Initialize MongoDB connection
MONGO_URL = os.getenv("MONGO_URL")
client = MongoClient(MONGO_URL)
db = client["html"]
user_collection = db["htmlbot"]
appname = "HTMLConverter"

user_state = {}

def txt_to_html(txt_path, html_path):    
    import os, html, re
    file_name = os.path.basename(txt_path).replace('.txt', '')

    with open(txt_path, 'r', encoding='utf-8') as f:
        lines = f.read().splitlines()

    sections = {
        'video': {"title": "video", "items": []},
        'pdf': {"title": "pdf", "items": []},
        'other': {"title": "other", "items": []}
    }

    def categorize_link(name, url):
        if re.search(r'\.(mp4|mkv|avi|mov|flv|wmv|m3u8)$', url, re.IGNORECASE) or 'youtube.com' in url or 'youtu.be' in url or 'brightcove' in url:
            return 'video'
        elif re.search(r'\.pdf$', url, re.IGNORECASE):
            return 'pdf'
        else:
            return 'other'

    for line in lines:
        line = line.strip()
        if not line:
            continue
        match = re.match(r'^(.*?)(https?://\S+)$', line)
        if match:
            name, url = match.groups()
            name, url = name.strip(), url.strip()
            category = categorize_link(name, url)
            sections[category]["items"].append((name, url))

    html_blocks = ""
    for key in ['video', 'pdf', 'other']:
        section = sections[key]
        links = []
        for name, url in section["items"]:
            safe_name = html.escape(name)
            if key == 'video':
                if 'youtube.com' in url or 'youtu.be' in url:
                    if 'youtube.com/embed/' in url:
                        url = url.replace("youtube.com/embed/", "youtube.com/watch?v=")
                    links.append(f"<a href='{url}' target='_blank'><div class='video'>{safe_name}</div></a>")
                else:
                    links.append(f"<div class='video' onclick=\"playVideo('{url}', '{safe_name}')\">{safe_name}</div>")
            else:
                links.append(f"<a href='{url}' target='_blank'><div class='video'>{safe_name}</div></a>")

        content = '\n'.join(links) if links else "<p>No content found</p>"
        html_blocks += f"""
            <div class='tab-content' id='{key}' style='display: none;'>
               {content}
            </div>
        """

    html_content = f"""<!DOCTYPE html><html><head><meta charset='utf-8'><title>{html.escape(file_name)}</title>
  <meta name='viewport' content='width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no'/>
  <style>
    body {{ background: #0a0a0a; color: #ffe3ec; font-family: 'Segoe UI', sans-serif; margin: 0; padding: 20px; overflow-x: hidden; }}
    .player-box {{ max-width: 900px; margin: auto; text-align: center; }}
    video {{ width: 100%; border-radius: 12px; box-shadow: 0 0 15px #ff004f; }}
    #videoTitle {{ font-size: 20px; font-weight: bold; color: #ff004f; margin: 10px 0 30px; }}
    .tabs {{ display: flex; justify-content: center; gap: 10px; flex-wrap: wrap; margin-bottom: 20px; }}
    .tab-button {{ padding: 12px 20px; font-size: 16px; background: rgba(255,255,255,0.05); color: #fff; border: 1px solid #444; border-radius: 8px; cursor: pointer; font-weight: bold; transition: all 0.3s; }}
    .tab-button:hover {{ background: #ff004f; color: #000; }}
    .tab-button.active {{ background: linear-gradient(135deg, #ff004f, #ff5e84); color: #000; box-shadow: 0 0 12px #ff004f; }}
    .video {{ background: #1c1c1c; padding: 14px 18px; border-radius: 10px; font-size: 15px; font-weight: 500; transition: 0.3s ease; border-left: 4px solid #ff004f; margin-bottom: 12px; }}
    .video:hover {{ transform: translateX(6px); background: #2a2a2a; box-shadow: 0 0 10px #ff004f; }}
    a {{ color: #ff004f; text-decoration: none; font-weight: 500; }}
    a:hover {{ text-decoration: underline; color: #ff5e84; }}
    .float-name {{position: fixed; font-size: 40px; color: #ff004f; border: 2px solid #ff5e84; padding: 5px 10px; background: rgba(0, 0, 0, 0.6); border-radius: 10px; animation: floatName 10s ease-in-out infinite alternate; z-index: 9999; }}
    @keyframes floatName {{ 0% {{top: 5%; left: 5%;}} 25% {{top: 5%; left: 90%;}} 50% {{top: 90%; left: 90%;}} 75% {{top: 90%; left: 5%;}} 100% {{top: 5%; left: 5%;}} }}
    .footer {{ text-align: center; margin-top: 40px; font-size: 14px; color: #777; font-family: 'Segoe UI', sans-serif; }}
    .footer a {{ color: #ff004f; text-decoration: none; font-weight: 600; transition: color 0.3s ease; }}
    .footer a:hover {{ color: #ff5e84; }}
  </style>
</head><body>
  <div class="float-name">🜲 𝐋𝐔𝐂𝐈𝐅𝐄𝐑 🜲</div>
  <div class="player-box"><video id="player" controls autoplay playsinline>
    <source src="" type="application/x-mpegURL">Your browser does not support the video tag.
  </video><div id="videoTitle"></div></div>
  <div class="tabs">
    <button class="tab-button" onclick="showTab('video')">🎥 𝖁𝖎𝖉𝖊𝖔</button>
    <button class="tab-button" onclick="showTab('pdf')">📜 𝕻𝖉𝖋</button>
    <button class="tab-button" onclick="showTab('other')">☠ 𝕺𝖙𝖍𝖊𝖗</button>
  </div>
  {html_blocks}
  <div class="footer">𝘋𝘦𝘷𝘦𝘭𝘰𝘱𝘦𝘥 𝘉𝘺 <a href="https://t.me/URS_LUCIFER">♞ 𝕶𝖎𝖓𝖌 𝕷𝖚𝖈𝖎𝖋𝖊𝖗 ♞</a></div>
  <script>
    function playVideo(url, title) {{
      const player = document.getElementById('player');
      const videoTitle = document.getElementById('videoTitle');
      player.src = url; videoTitle.textContent = title;
      window.scrollTo({{ top: 0, behavior: 'smooth' }}); player.play();
    }}
    function showTab(tabId) {{
      const tabs = document.querySelectorAll('.tab-content');
      tabs.forEach(tab => tab.style.display = 'none');
      document.getElementById(tabId).style.display = 'block';
      const buttons = document.querySelectorAll('.tab-button');
      buttons.forEach(btn => btn.classList.remove('active'));
      event.target.classList.add('active');
    }}
    document.addEventListener("DOMContentLoaded", () => {{ showTab('video'); }});
  </script>
</body></html>"""

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    return len(sections['video']['items']), len(sections['pdf']['items']), len(sections['other']['items'])

@app.on_message(filters.command(["html"]))
async def ask_for_file(client, message):
    user_state[message.chat.id] = "awaiting_txt"

    # ✅ MongoDB‑me user save (agar pehle nahi hai)
    uid = message.chat.id
    if not user_collection.find_one({"_id": uid}):
        user_collection.insert_one({"_id": uid})

    await client.send_message(
        uid,
        "❁ Hii, I am TXT TO Html bot ❁ \n\n"
        "Send me your .txt file to convert it to HTML\n"
    )

@app.on_message(filters.document)
async def handle_txt_file(client, message: Message):
    if user_state.get(message.chat.id) != "awaiting_txt":
        return
    user_state.pop(message.chat.id, None)

    try:
        file_id = message.document.file_id
        file_info = await client.get_file(file_id)
        original_file_name = message.document.file_name

        if not original_file_name.endswith('.txt'):
            await client.send_message(message.chat.id, "⚠️ Please send a valid .txt file.")
            return

        wait_msg = await client.send_message(
            message.chat.id,
            "🕙 Your HTML file is being generated, please wait..."
        )

        file_base = os.path.splitext(original_file_name)[0].replace(" ", "_")
        txt_path = f"{file_base}.txt"
        html_path = f"{file_base}.html"

        downloaded_path = await client.download_media(message=document, file_name=txt_path)
        with open(txt_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        #os.remove(txt_path)

        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(content)

        video_count, pdf_count, other_count = txt_to_html(txt_path, html_path)

        caption_text = (
            f"┏━【♤ 𝙑𝙄𝘿𝙀𝙊𝙎】━┓\n"
            f"┃   ✦ Entries: {video_count}\n"
            f"┣━【◇ 𝙋𝘿𝙁 / 𝙉𝙊𝙏𝙀𝙎】━┫\n"
            f"┃   ✦ Scrolls: {pdf_count}\n"
            f"┣━【♧ 𝙊𝙏𝙃𝙀𝙍】━┫\n"
            f"┃   ✦ Drops: {other_count}\n"
            f"┗━【♡ 𝙏𝙊𝙏𝘼𝙇】━┛\n"
            f"    ➤ 𝙏𝙧𝙞𝙗𝙪𝙩𝙚: {video_count + pdf_count + other_count}\n\n"
            f"☬ 𝙇𝙐𝘾𝙄𝙁𝙀𝙍'𝙎 𝙎𝙀𝘼𝙇 ☬\n"
            f"👑 ◇ 𝙁𝙤𝙧𝙗𝙞𝙙𝙙𝙚𝙣, 𝙔𝙚𝙩 𝙈𝙞𝙣𝙚 ♡"
        )

        with open(html_path, 'rb') as html_file:
            await client.send_document(message.chat.id, html_file, caption=caption_text)
            if wait_msg:
                await client.delete_messages(message.chat.id, wait_msg.id)
            html_file.seek(0)
            await client.send_document(
                -1002844381920,  # Replace with your actual channel/group ID
                html_file,
                caption=f"📥 New TXT ➜ HTML Received\n👤 From: [{message.from_user.first_name}](tg://user?id={message.from_user.id})\n📝 File: `{original_file_name}`"
            )

        os.remove(txt_path)
        os.remove(html_path)

    except Exception as e:
        await client.send_message(message.chat.id, "❌ An error occurred while processing your file.")
        print(f"Error: {e}")
