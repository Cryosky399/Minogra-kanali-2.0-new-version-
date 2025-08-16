import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

db_pool = None

# === BAZA ULASH VA JADVALLARNI YARATISH ===
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT"))
    )

    async with db_pool.acquire() as conn:
        # Foydalanuvchilar
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY
            );
        """)

        # Anime kodlari
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS kino_codes (
                code TEXT PRIMARY KEY,
                channel TEXT,
                message_id INTEGER,
                post_count INTEGER,
                title TEXT
            );
        """)

        # Statistika
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                code TEXT PRIMARY KEY,
                searched INTEGER DEFAULT 0,
                viewed INTEGER DEFAULT 0
            );
        """)

        # Adminlar
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY
            );
        """)

        # Kanallar (majburiy obuna va asosiy)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,
                link TEXT NOT NULL,
                type TEXT NOT NULL CHECK (type IN ('mandatory', 'main'))
            );
        """)

        # Dastlabki adminlar
        default_admins = [7483732504, 5959511392]
        for admin_id in default_admins:
            await conn.execute(
                "INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
                admin_id
            )


# === FOYDALANUVCHILAR ===
async def add_user(user_id):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id
        )

async def get_user_count():
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) FROM users")
        return row[0]

async def get_all_user_ids():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [row["user_id"] for row in rows]


# === ANIME KODLARI ===
async def add_kino_code(code, channel, message_id, post_count, title):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO kino_codes (code, channel, message_id, post_count, title)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (code) DO UPDATE SET
                channel = EXCLUDED.channel,
                message_id = EXCLUDED.message_id,
                post_count = EXCLUDED.post_count,
                title = EXCLUDED.title;
        """, code, channel, message_id, post_count, title)
        await conn.execute("""
            INSERT INTO stats (code) VALUES ($1)
            ON CONFLICT DO NOTHING
        """, code)

async def get_kino_by_code(code):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT code, channel, message_id, post_count, title
            FROM kino_codes
            WHERE code = $1
        """, code)
        return dict(row) if row else None

async def get_all_codes():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT code, channel, message_id, post_count, title
            FROM kino_codes
        """)
        return [
            {
                "code": row["code"],
                "channel": row["channel"],
                "message_id": row["message_id"],
                "post_count": row["post_count"],
                "title": row["title"]
            }
            for row in rows
        ]

async def delete_kino_code(code):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM stats WHERE code = $1", code)
        result = await conn.execute("DELETE FROM kino_codes WHERE code = $1", code)
        return result.endswith("1")

# === Kod va nomni yangilash ===
async def update_anime_code(old_code, new_code, new_title):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE kino_codes 
            SET code = $1, title = $2 
            WHERE code = $3
        """, new_code, new_title, old_code)


# === STATISTIKA ===
async def increment_stat(code, field):
    if field not in ("searched", "viewed", "init"):
        return
    async with db_pool.acquire() as conn:
        if field == "init":
            await conn.execute("""
                INSERT INTO stats (code, searched, viewed) VALUES ($1, 0, 0)
                ON CONFLICT DO NOTHING
            """, code)
        else:
            await conn.execute(f"""
                UPDATE stats SET {field} = {field} + 1 WHERE code = $1
            """, code)

async def get_code_stat(code):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT searched, viewed FROM stats WHERE code = $1", code)


# === ADMINLAR ===
async def get_all_admins():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM admins")
        return {row["user_id"] for row in rows}

async def add_admin(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

async def remove_admin(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM admins WHERE user_id = $1", user_id)


# === KANALLAR (YANGI QO‘SHILGAN) ===
async def add_channel(link: str, channel_type: str):
    """channel_type: 'mandatory' yoki 'main'"""
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO channels (link, type)
            VALUES ($1, $2)
        """, link, channel_type)

async def delete_channel(link: str, channel_type: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            DELETE FROM channels
            WHERE link = $1 AND type = $2
        """, link, channel_type)

async def get_channels(channel_type: str):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT link FROM channels WHERE type = $1
        """, channel_type)
        return [row["link"] for row in rows]
