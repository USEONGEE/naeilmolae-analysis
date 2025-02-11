import difflib
import subprocess
import os
import boto3
from pydub import AudioSegment
from confluent_kafka import Consumer, Producer, KafkaException
import json
import logging
from dotenv import load_dotenv
import requests

# 환경변수 로드 및 로깅 설정
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="app.log",
    filemode="a",
)
file_handler = logging.FileHandler("app.log")
error_handler = logging.FileHandler("error.log")
file_handler.setLevel(logging.INFO)
error_handler.setLevel(logging.ERROR)
logger = logging.getLogger()
logger.addHandler(file_handler)
logger.addHandler(error_handler)

# 환경변수 읽기
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")
REQUEST_TOPIC = os.getenv("REQUEST_TOPIC")
RESULT_TOPIC = os.getenv("RESULT_TOPIC")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")


###############################################################################
# Analyzer 클래스: 텍스트 유사도 분석 및 부적절한 내용 감지 기능
###############################################################################
class Analyzer:
    @staticmethod
    def check_similarity(text1, text2, threshold=0.85):
        similarity = difflib.SequenceMatcher(None, text1, text2).ratio()
        is_similar = similarity >= threshold
        return is_similar, similarity

    @staticmethod
    def detect_inappropriate_content(text):
        # TODO: 부적절한 내용 탐지 로직 구현
        # 예: inappropriate_keywords = ["금지된 단어1", "금지된 단어2"]
        # for keyword in inappropriate_keywords:
        #     if keyword in text:
        #         return True
        return False


###############################################################################
# AudioProcessor 클래스: S3/HTTPS에서 파일 다운로드, 음원 변환 및 STT 처리
###############################################################################
class AudioProcessor:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def stt(self, filepath):
        print("stt 수행중")
        try:
            result = subprocess.run(
                [
                    os.path.join(os.path.dirname(__file__), "whisper.cpp", "main"),
                    "-l",
                    "ko",
                    "-m",
                    os.path.join(
                        os.path.dirname(__file__),
                        "whisper.cpp",
                        "models",
                        "ggml-model.bin",
                    ),
                    "-f",
                    filepath,
                    "-nt",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr)
            return result.stdout.strip()
        except Exception as e:
            raise RuntimeError(f"음성 파일 변환 중 오류 발생: {e}")
        finally:
            print("stt 수행완료")

    def get_wav_file_from_s3(self, file_url):
        try:
            # 로컬 파일 경로 설정
            local_wav_path = os.path.join(
                os.path.dirname(__file__), "music", os.path.basename(file_url)
            )
            if file_url.startswith("s3://"):
                bucket_name, key = self.parse_s3_url(file_url)
                self.s3_client.download_file(bucket_name, key, local_wav_path)
            else:
                self.download_file_from_https(file_url, local_wav_path)

            # 16kHz로 변환
            converted_wav_path = local_wav_path.replace(".wav", "_16k.wav")
            audio = AudioSegment.from_file(local_wav_path, format="wav")
            audio = audio.set_frame_rate(16000)
            audio.export(converted_wav_path, format="wav")
            return converted_wav_path
        except Exception as e:
            raise RuntimeError(f"S3에서 파일을 가져오는 중 오류 발생: {e}")

    @staticmethod
    def download_file_from_https(url, local_path):
        response = requests.get(url)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(response.content)

    @staticmethod
    def parse_s3_url(s3_url):
        if not s3_url.startswith("s3://"):
            raise ValueError("올바른 S3 URL이 아닙니다.")
        parts = s3_url[5:].split("/", 1)
        if len(parts) != 2:
            raise ValueError("S3 URL 형식이 올바르지 않습니다.")
        return parts[0], parts[1]


###############################################################################
# MessageProcessor 클래스: Kafka로 전달된 메시지를 처리하여 STT 및 분석 수행
###############################################################################
class MessageProcessor:
    def __init__(self, audio_processor, analyzer):
        self.audio_processor = audio_processor
        self.analyzer = analyzer

    def process_message(self, key, message):
        try:
            file_url = message.get("fileUrl")
            original_text = message.get("content")

            # S3/HTTPS에서 파일 다운로드 및 16kHz 변환
            wav_file_path = self.audio_processor.get_wav_file_from_s3(file_url)

            # STT 처리
            stt_text = self.audio_processor.stt(wav_file_path)
            print(f"STT 결과: {stt_text}")

            # 유사도 분석
            is_similar, similarity = self.analyzer.check_similarity(
                original_text, stt_text
            )

            # 부적절한 내용 감지
            if self.analyzer.detect_inappropriate_content(stt_text):
                analysis_result_status = "INCLUDE_INAPPROPRIATE_CONTENT"
            elif not is_similar:
                analysis_result_status = "NOT_READ_VOICE"
            else:
                analysis_result_status = "SUCCESS"

            return key, {
                "analysisResultStatus": analysis_result_status,
                "sttContent": stt_text,
            }
        except Exception as e:
            logger.error(f"메시지 처리 중 오류 발생: {e}")
            return key, {
                "analysisResultStatus": "ERROR",
                "sttContent": str(e),
            }


###############################################################################
# KafkaClient 클래스: Kafka Producer를 감싸고 메시지 전송 기능 제공
###############################################################################
class KafkaClient:
    def __init__(self, bootstrap_servers, result_topic):
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.result_topic = result_topic

    def produce_message(self, key, value, message_type):
        try:
            headers = [("spring.kafka.type", message_type)]
            self.producer.produce(
                self.result_topic,
                key=key,
                value=json.dumps(value),
                headers=headers,
                callback=lambda err, msg: print(
                    f"Message delivery failed: {err}"
                    if err
                    else f"Message delivered to {msg.topic()} [{msg.partition()}]"
                ),
            )
            self.producer.flush()
        except Exception as e:
            logger.error(f"Kafka 메시지 전송 중 오류 발생: {e}")


###############################################################################
# 유틸리티 함수: Kafka 메시지 헤더에서 특정 키 추출
###############################################################################
def get_header(headers, key):
    for header_key, header_value in headers:
        if header_key == key:
            return header_value.decode("utf-8")
    return None


###############################################################################
# 메인 함수: Kafka Consumer를 통해 메시지 수신 후 처리하고 결과를 Kafka로 전송
###############################################################################
def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([REQUEST_TOPIC])

    # S3 클라이언트 생성
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    # 각 기능별 클래스 인스턴스 생성
    audio_processor = AudioProcessor(s3_client)
    analyzer = Analyzer()
    message_processor = MessageProcessor(audio_processor, analyzer)
    kafka_client = KafkaClient(KAFKA_BOOTSTRAP_SERVERS, RESULT_TOPIC)

    try:
        while True:
            try:
                msg = consumer.poll(1.0)  # 1초 대기
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        continue

                key = msg.key().decode("utf-8")
                value = json.loads(msg.value().decode("utf-8"))
                headers = msg.headers()
                message_type = get_header(headers, "requiredResponseType")
                if message_type is None:
                    print("requiredResponseType header not found")

                print(f"Received message with key: {key}, value: {value}")

                response_key, response_value = message_processor.process_message(
                    key, value
                )
                print(
                    f"Sending response with key: {response_key}, value: {response_value}"
                )

                kafka_client.produce_message(response_key, response_value, message_type)
            except Exception as e:
                logger.error(f"메시지 처리 중 오류 발생: {e}")
                continue
    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
