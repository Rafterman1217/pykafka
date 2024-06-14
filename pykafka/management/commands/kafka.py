
import logging
from django.core.management.base import BaseCommand
from pykafka.runtime import PyKafka
from django.conf import settings

class Command(BaseCommand):

    def handle(self, *args, **options):
        logging.basicConfig(level=logging.INFO)
        PyKafka.django_init(settings.BASE_DIR,settings.KAFKA_BOOTSTRAP_SERVERS).run()