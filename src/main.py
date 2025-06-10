import asyncio
from src.data_pipeline.backfill import Backfill
from src.data_pipeline.check_gaps import check_todays_gaps
from src.test_pipeline import test_pipeline
from src.utils.logger import get_logger

logger = get_logger(__name__)

def display_menu():
    print("\nMACD Pipeline Options:")
    print("1. Historical Backfill (up to yesterday)")
    print("2. Check Today's Gaps (post-market close)")
    print("3. Real-Time Tick Processing (during market hours)")
    print("4. Exit")
    return input("Enter your choice (1-4): ")

async def run_historical_backfill():
    try:
        backfill = Backfill()
        lookback_days = int(input("Enter lookback days (max 100, default 7): ") or 7)
        if lookback_days > 100:
            logger.warning("Lookback capped at 100 days due to API limits.")
            lookback_days = 100
        logger.info(f"Starting historical backfill for {lookback_days} days.")
        await backfill.backfill_all(lookback_days=lookback_days, today_only=False)
        logger.info("Historical backfill completed.")
    except Exception as e:
        logger.error(f"Historical backfill failed: {e}")

async def run_gap_check():
    try:
        logger.info("Starting today's gap check.")
        await check_todays_gaps()
        logger.info("Gap check completed.")
    except Exception as e:
        logger.error(f"Gap check failed: {e}")

def run_real_time_processing():
    try:
        logger.info("Starting real-time tick processing.")
        test_pipeline(override_market_check=True)  # Allow testing outside market hours
        logger.info("Real-time processing completed.")
    except Exception as e:
        logger.error(f"Real-time processing failed: {e}")

def main():
    while True:
        choice = display_menu()
        if choice == "1":
            asyncio.run(run_historical_backfill())
        elif choice == "2":
            asyncio.run(run_gap_check())
        elif choice == "3":
            run_real_time_processing()
        elif choice == "4":
            logger.info("Exiting MACD Pipeline.")
            break
        else:
            print("Invalid choice. Please enter 1, 2, 3, or 4.")

if __name__ == "__main__":
    main()