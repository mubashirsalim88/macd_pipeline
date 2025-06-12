import asyncio
import time
from datetime import datetime, timedelta
import pytz
from src.data_pipeline.backfill import Backfill
from src.data_pipeline.check_gaps import check_todays_gaps
from src.test_pipeline import test_pipeline
from src.utils.logger import get_logger

logger = get_logger(__name__)

def is_market_open():
    now = datetime.now(pytz.timezone("Asia/Kolkata"))
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    is_open = market_open <= now <= market_close
    logger.info(f"Market status: {'Open' if is_open else 'Closed'} at {now}")
    return is_open

def wait_for_market_open():
    now = datetime.now(pytz.timezone("Asia/Kolkata"))
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now.date() > market_open.date():  # If after 9:15 AM, set to next day's 9:15 AM
        market_open = market_open + timedelta(days=1)
    if now < market_open:
        seconds_to_wait = (market_open - now).total_seconds()
        logger.info(f"Waiting {seconds_to_wait:.0f} seconds until market open at {market_open}")
        while seconds_to_wait > 0:
            minutes, seconds = divmod(int(seconds_to_wait), 60)
            print(f"Market opens in {minutes:02d}:{seconds:02d}...", end="\r")
            time.sleep(min(seconds_to_wait, 1))
            seconds_to_wait -= 1
            now = datetime.now(pytz.timezone("Asia/Kolkata"))
            if now.date() > market_open.date():  # Handle date change
                market_open = now.replace(hour=9, minute=15, second=0, microsecond=0) + timedelta(days=1)
                seconds_to_wait = (market_open - now).total_seconds()
        print(" " * 50, end="\r")  # Clear countdown line
    logger.info("Market open. Starting pipeline.")

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
        logger.error(f"Historical backfill failed: {e}", exc_info=True)

async def run_gap_check():
    try:
        logger.info("Starting today's gap check.")
        await check_todays_gaps()
        logger.info("Gap check completed.")
    except Exception as e:
        logger.error(f"Gap check failed: {e}", exc_info=True)

async def run_real_time_processing():
    try:
        logger.info("Starting real-time tick processing.")
        if not is_market_open():
            wait_for_market_open()
        await test_pipeline(override_market_check=False)  # Await the coroutine
        logger.info("Real-time processing completed.")
    except Exception as e:
        logger.error(f"Real-time processing failed: {e}", exc_info=True)

def main():
    loop = asyncio.get_event_loop()
    while True:
        choice = display_menu()
        try:
            if choice == "1":
                loop.run_until_complete(run_historical_backfill())
            elif choice == "2":
                loop.run_until_complete(run_gap_check())
            elif choice == "3":
                loop.run_until_complete(run_real_time_processing())
            elif choice == "4":
                logger.info("Exiting MACD Pipeline.")
                break
            else:
                print("Invalid choice. Please enter 1, 2, 3, or 4.")
        except Exception as e:
            logger.error(f"Main loop error: {e}", exc_info=True)
        finally:
            if loop.is_running():
                loop.stop()
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

if __name__ == "__main__":
    main()