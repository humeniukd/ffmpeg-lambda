from google.cloud import storage, pubsub_v1
from subprocess import Popen, PIPE
from datetime import datetime, timedelta
from re import U, I, compile as recompile
from os import remove, environ
from math import floor
import csv, json

s3 = storage.Client()
publisher = pubsub_v1.PublisherClient()

LOCATION_ID = '' #TODO
PROJECT_ID = '' #TODO
HEIGHT = 140
WIDTH = 1800

OUT_BUCKET_NAME = environ['BUCKET']

WORK_DIR = environ.get('WORK_DIR', '/tmp/')

re_duration = recompile('Duration: (\d{2}):(\d{2}):(\d{2}).(\d{2})[^\d]*', U)
re_freq = recompile('(\d+) Hz', U | I)
re_position = recompile('out_time_ms=(\d+)\d{3}', U)

delta = timedelta(seconds=2)

def time2ms(s):
    hours = 3600000 * int(s.group(1))
    minutes = 60000 * int(s.group(2))
    seconds = 1000 * int(s.group(3))
    ms = 10 * int(s.group(4))
    return hours + minutes + seconds + ms

def ratio(position, duration):
    if not position or not duration:
        return 0
    percent = int(floor(100 * position / duration))
    return 100 if percent > 100 else percent

class WfThread(object):
    global WORK_DIR, WIDTH, HEIGHT
    __SQSQueue = None
    __inBucket = None

    @property
    def SQSQueue(self):
        if None == self.__SQSQueue:
            pass
        return self.__SQSQueue

    def __init__(self, key=None, bucket=None):
        self.__key = key
        self.__outFile = key + '.mp3'
        self.__csvFile = key + '.csv'
        self.__jsonFile = key + '.json'
        self.__inBucket = s3.bucket(bucket)
        self.__topic = publisher.topic_path(PROJECT_ID, self.__key)
        publisher.create_topic(name=self.__topic)
        self.__ts = datetime.now()

    def __del__(self):
        for fileName in [self.__csvFile, self.__key, self.__outFile]:
            remove(WORK_DIR + fileName)
        blob = self.__inBucket.blob(self.__key)
        blob.delete()
        print('Destruct %s', self.__key)

    def run(self):
        for fn in [self.__download, self.__probe, self.__process, self.__convert, self.__upload]:
            if not fn():
                self.__enqueue('{"key": "error"}')
                break

    def __probe(self):
        process = Popen([
            './ffprobe',
            '-i', WORK_DIR + self.__key
        ], stdout=PIPE, stderr=PIPE, bufsize=1)

        while True:
            output = process.stderr.readline().decode('utf-8')
            rc = process.poll()

            if output == '' and rc is not None:
                break

            freq_match = re_freq.search(output)
            if freq_match:
                self.__freq = int(freq_match.group(1))

            duration_match = re_duration.search(output)
            if duration_match:
                self.__duration = time2ms(duration_match)

        print('probe RC: %d', rc)
        return 0 == rc

    def __process(self):
        spl = self.__duration * self.__freq / 1000 / WIDTH
        print('duration: %d, freq: %d, spl: %d', self.__duration, self.__freq, spl)
        process = Popen([
            './ffmpeg',
            '-i', WORK_DIR + self.__key,
            '-map', '0:0',
            '-progress', '/dev/stderr',
            '-af', 'dumpwave=w=%d:n=%d:f=%s' % (WIDTH, spl, WORK_DIR + self.__csvFile),
                  WORK_DIR + self.__outFile
        ], stdout=PIPE, stderr=PIPE, bufsize=1)

        ms = None
        percent = 0

        while True:
            output = process.stderr.readline().decode('utf-8')
            rc = process.poll()

            if output == '' and rc is not None:
                break

            if self.__duration:
                position_match = re_position.search(output)
                if position_match:
                    ms = int(position_match.group(1))

            _percent = ratio(ms, self.__duration)

            if _percent != percent:
                percent = _percent
                self.__enqueue('{"type": "percent", "value": %(value)d}' % {'value': percent})
        isOk = 0 == rc
        if isOk:
            self.__enqueue('{"type": "percent", "value": %(value)d}' % {'value': 100}, True)

        else:
            self.__enqueue('{"type": "error"}')
        print('RC: %d', rc)
        return isOk

    def __enqueue(self, msg, force=False):
        now = datetime.now()
        if force or now - self.__ts > delta:
            self.__ts = now
            response = publisher.publish(self.__topic, msg)
            pass

    def __download(self):
        try:
            blob = self.__inBucket.blob(self.__key)
            blob.download_to_filename(WORK_DIR + self.__key)

        except Exception as e:
            print('Download failed %s', str(e))
            return False
        print('Downloaded %s', self.__key)
        return True

    def __convert(self):
        try:
            data = []
            with open(WORK_DIR + self.__csvFile) as csvFile:
                csvReader = csv.DictReader(csvFile)
                for v in csvReader.fieldnames:
                    data.append(int(float(v) * HEIGHT))
            with open(WORK_DIR + self.__jsonFile, 'w') as jsonFile:
                jsonFile.write(json.dumps({
                    'width': WIDTH,
                    'height': HEIGHT,
                    'samples': data
                }))
        except Exception as e:
            print('Convert failed %s', str(e))
            return False
        print('Converted %s', self.__key)
        return True

    def __upload(self):
        global WORK_DIR, OUT_BUCKET_NAME
        bucket = s3.bucket(OUT_BUCKET_NAME)
        for fileName in [self.__jsonFile, self.__outFile]:
            try:
                blob = bucket.blob(fileName)
                blob.upload_from_filename(WORK_DIR + fileName)
            except Exception as e:
                print('Upload failed: %s', str(e))
                return False
            print('Uploaded %s', fileName)
        return True


def handler(event, context):
    key = event['name']
    bucket = event['bucket']
    WfThread(key, bucket).run()
    return 'ok'
