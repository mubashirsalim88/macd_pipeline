from apscheduler.schedulers.background import BackgroundScheduler
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Scheduler:
    def __init__(self):
        """
        Initialize scheduler for task orchestration.
        """
        self.scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

    def add_job(self, func, interval, **kwargs):
        """
        Add a job to the scheduler.
        """
        self.scheduler.add_job(func, "interval", seconds=interval, **kwargs)
        logger.info(f"Scheduled job: {func.__name__} every {interval}s")

    def start(self):
        """
        Start the scheduler.
        """
        try:
            self.scheduler.start()
            logger.info("Scheduler started")
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")

    def shutdown(self):
        """
        Stop the scheduler.
        """
        try:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")