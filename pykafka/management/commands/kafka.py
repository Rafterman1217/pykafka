
import logging
from django.core.management.base import BaseCommand
from pykafka.runtime import PyKafka
from django.conf import settings


class Command(BaseCommand):
    """
    A custom management command for handling Kafka operations.
    """

    def handle(self, *args, **options):
        """
        The main entry point for the management command.
        This method is called when the command is executed.
        """
        logging.basicConfig(level=logging.INFO)
        PyKafka.django_init(settings.BASE_DIR,
                            settings.KAFKA_BOOTSTRAP_SERVERS).run()
