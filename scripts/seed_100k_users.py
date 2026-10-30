import asyncio
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from faker import Faker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.config import settings
from app.models.users import User

fake = Faker()

COUNTRIES = ["IN", "US", "GB", "DE", "SG", "CA", "AE"]
CITIES_BY_COUNTRY = {
    "IN": ["Mumbai", "Bengaluru", "Delhi", "Hyderabad", "Pune", "Chennai"],
    "US": ["San Francisco", "New York", "Austin", "Seattle", "Chicago"],
    "GB": ["London", "Manchester", "Edinburgh", "Birmingham"],
    "DE": ["Berlin", "Munich", "Frankfurt", "Hamburg"],
    "SG": ["Singapore"],
    "CA": ["Toronto", "Vancouver", "Montreal"],
    "AE": ["Dubai", "Abu Dhabi"],
}
TIERS = ["free", "starter", "pro", "whale"]
TIER_WEIGHTS = [0.70, 0.20, 0.08, 0.02]


def generate_user_batch(batch_size: int, offset: int) -> list[dict]:
    batch = []
    now = datetime.now(timezone.utc)
    for i in range(batch_size):
        idx = offset + i
        country = random.choice(COUNTRIES)
        city = random.choice(CITIES_BY_COUNTRY.get(country, ["Central"]))
        tier = random.choices(TIERS, weights=TIER_WEIGHTS)[0]
        
        # Spend correlates with tier
        if tier == "whale":
            lifetime_spend = round(random.uniform(5000, 25000), 2)
        elif tier == "pro":
            lifetime_spend = round(random.uniform(500, 4999), 2)
        elif tier == "starter":
            lifetime_spend = round(random.uniform(50, 499), 2)
        else:
            lifetime_spend = round(random.uniform(0, 49), 2)

        days_inactive = random.choices([0, 1, 3, 7, 14, 30, 60], weights=[0.3, 0.2, 0.15, 0.15, 0.1, 0.05, 0.05])[0]
        last_active = now - timedelta(days=days_inactive, minutes=random.randint(0, 1440))
        created_at = last_active - timedelta(days=random.randint(10, 365))

        user_data = {
            "id": uuid.uuid4(),
            "external_user_id": f"usr_{idx}_{uuid.uuid4().hex[:6]}",
            "email": f"user_{idx}_{fake.user_name()}@{fake.free_email_domain()}",
            "phone_e164": f"+91{random.randint(7000000000, 9999999999)}" if country == "IN" else f"+1{random.randint(2000000000, 9999999999)}",
            "fcm_token": f"fcm_token_{uuid.uuid4().hex}_{idx}",
            "country": country,
            "last_active_at": last_active,
            "metadata_": {
                "city": city,
                "tier": tier,
                "lifetime_spend": lifetime_spend,
                "app_version": random.choice(["2.4.0", "2.5.1", "3.0.0"]),
                "push_opt_in": random.random() > 0.15,
                "preferred_language": "hi" if country == "IN" and random.random() > 0.5 else "en",
            },
            "created_at": created_at,
        }
        batch.append(user_data)
    return batch


async def seed_users(total_count: int = 100000, batch_size: int = 2500):
    print(f"🚀 Starting seed generation for {total_count:,} users in batches of {batch_size:,}...")
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    start_time = time.time()
    inserted = 0

    for offset in range(0, total_count, batch_size):
        batch = generate_user_batch(batch_size, offset)
        users = [User(**u) for u in batch]
        async with session_factory() as session:
            async with session.begin():
                session.add_all(users)
        inserted += len(users)
        elapsed = time.time() - start_time
        rate = inserted / elapsed if elapsed > 0 else 0
        print(f"[{inserted:,}/{total_count:,}] inserted... ({rate:,.0f} users/sec)")

    total_time = time.time() - start_time
    print(f"✅ Finished seeding {inserted:,} users in {total_time:.2f} seconds ({inserted / total_time:,.0f} users/sec)!")
    await engine.dispose()


if __name__ == "__main__":
    import sys
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
    asyncio.run(seed_users(total_count=count))
