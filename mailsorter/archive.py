import datetime
import email.utils
import hashlib
import logging
import mailbox
import os
import os.path
import pickle
import re
import sys


class DuplicateError(Exception):
    """
    Raised when a message's date cannot be determined.
    """
    pass


class UndatedError(Exception):
    """
    Raised when a message is already stored in an archive.
    """
    pass


class MailArchive:
    """
    Disk-based e-mail storage.

    Instances of this class must be accessed as context managers.
    """

    SEEN_FNAME = 'seen.pickle'

    def __init__(self, directory):
        """
        Initializes the storage.
        :param directory: Directory in which the archive is stored.
        The directory and all the parent directories will be created,
        if necessary.
        """
        self.logger = logging.getLogger(__name__)
        os.makedirs(directory, exist_ok=True)
        self.directory = directory

    def __enter__(self):
        self.boxes = {}
        seen_path = self.directory + os.sep + self.SEEN_FNAME
        if os.path.exists(seen_path):
            with open(seen_path, 'rb') as f:
                self.seen = pickle.load(f)
        else:
            self.seen = set()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for outbox in self.boxes.values():
            outbox.close()
        del self.boxes
        seen_path = self.directory + os.sep + self.SEEN_FNAME
        with open(seen_path, 'wb') as f:
            pickle.dump(self.seen, f)
        del self.seen
        return False

    def _get_box_by_name(self, name):
        """
        Returns a mailbox for e-mails with the given file name.
        :param name: File name of the mailbox.
        :return: The right mailbox, as a `mailbox.Mailbox` object.
        """
        outbox = self.boxes.get(name, None)
        if outbox:
            return outbox
        outbox = mailbox.mbox(self.directory + os.sep + name)
        self.boxes[name] = outbox
        return outbox

    def _get_box_by_timestamp(self, timestamp):
        """
        Returns a mailbox for e-mails with the given timestamp.
        :param timestamp: Timestamp as a `datetime.datetime` object.
        :return: The right mailbox, as a `mailbox.Mailbox` object.
        """
        name = "%04d%02d" % (timestamp.year, timestamp.month)
        return self._get_box_by_name(name)

    def add(self, msg):
        """
        Adds a message to the mail archive.
        :param msg: A message to add, as an `email.Message` object.
        :raises UndatedError: When the message's date cannot be determined.
        :raises DuplicateError: When the message is already stored in the archive.
        """
        content = bytes(msg)

        md5 = hashlib.md5()
        md5.update(content)
        md5 = md5.hexdigest()

        if md5 in self.seen:
            raise DuplicateError
        self.seen |= {md5}

        timestamp = msg.get('Date')
        if not timestamp:
            raise UndatedError
        timestamp = email.utils.parsedate_tz(timestamp)
        if not timestamp:
            raise UndatedError
        timestamp = email.utils.mktime_tz(timestamp)
        timestamp = datetime.datetime.utcfromtimestamp(timestamp)

        outbox = self._get_box_by_timestamp(timestamp)
        outbox.add(msg)

    def __iter__(self):
        """
        Iterates over all the messages in the archive.
        :return: An iterator over all the messages.
        """
        for name in os.listdir(self.directory):
            if not re.match(r'[12][0-9]{5}', name):
                continue
            box = self._get_box_by_name(name)
            for msg in box:
                yield msg


def process(archive_dir, *entries):
    """
    Builds an e-mail archive from provided mailbox files.
    :param archive_dir: Directory in which the archive is stored.
    :param entries: Names of files or directories with mailboxes in mbox format.
    """
    logger = logging.getLogger(__name__)
    for entry in entries:
        if os.path.isdir(entry):
            for dirname, dirnames, filenames in os.walk(entry):
                for filename in filenames:
                    process(archive_dir, os.path.join(dirname, filename))
        else:
            box = mailbox.mbox(entry)
            with MailArchive(archive_dir) as archive:
                ok, duplicate, error = 0, 0, 0
                # Tried for msg in box, but it threw UnicodeDecodeError
                # for some mailboxes. Obviously, we want to catch that
                # exception per message not per mailbox.
                for key in box.iterkeys():
                    try:
                        msg = box[key]
                        archive.add(msg)
                        ok += 1
                    except KeyError:
                        continue
                    except UnicodeDecodeError:
                        error += 1
                    except DuplicateError:
                        duplicate += 1
                    except UndatedError:
                        error += 1
                logger.info("%s: %d OK, %d duplicate(s), %d error(s)" % (entry, ok, duplicate, error))
                sys.stdout.flush()


def export(archive_dir, output=sys.stdout):
    """
    Exports basic message metadata in CSV format.
    :param archive_dir: Directory in which the archive is stored.
    :param output: Output file (defaults to standard output).
    """
    with MailArchive(archive_dir) as archive:
        output.write('timestamp,sender,recipients,size,list\n')
        for msg in archive:
            sender = msg.get('From')
            if not sender:
                continue
            _, sender = email.utils.parseaddr(str(sender))
            sender = sender.lower()

            timestamp = msg.get('Date')
            timestamp = email.utils.parsedate_tz(timestamp)
            timestamp = email.utils.mktime_tz(timestamp)
            timestamp = datetime.datetime.utcfromtimestamp(timestamp)
            timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            size = len(bytes(msg))

            is_list = 'List-Id' in msg

            recipients = map(str, msg.get_all('To', []) + msg.get_all('Cc', []))
            recipients = email.utils.getaddresses(recipients)
            recipients = list(map(lambda p: p[1].lower(), recipients))

            output.write('%s,%s,%s,%d,%d\n' % (timestamp, sender, ' '.join(recipients), size, is_list))
