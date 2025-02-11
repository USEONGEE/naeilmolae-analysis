import difflib
import subprocess
import os
import boto3
from pydub import AudioSegment
from confluent_kafka import Consumer, Producer, KafkaException
import json
import logging
from dotenv import load_dotenv
import base64

#### init START
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename="app.log",
    filemode="a",  # 'a' for append, 'w' for overwrite
)

file_handler = logging.FileHandler("app.log")
error_handler = logging.FileHandler("error.log")
file_handler.setLevel(logging.INFO)
error_handler.setLevel(logging.ERROR)

# 로거에 핸들러 추가
logger = logging.getLogger()  # 루트 로거 가져오기
logger.addHandler(file_handler)
logger.addHandler(error_handler)

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP")
REQUEST_TOPIC = os.getenv("REQUEST_TOPIC")
RESULT_TOPIC = os.getenv("RESULT_TOPIC")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")


# S3 설정
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
#### init END


def check_similarity(text1, text2, threshold=0.85):
    similarity = difflib.SequenceMatcher(None, text1, text2).ratio()
    is_similar = similarity >= threshold
    return is_similar, similarity


def stt(filepath):
    print("stt 수행중")
    try:
        result = subprocess.run(
            [
                os.path.join(os.path.dirname(__file__), "whisper.cpp", "main"),
                "-l",
                "ko",
                "-m",
                os.path.join(
                    os.path.dirname(__file__), "whisper.cpp", "models", "ggml-model.bin"
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
            raise RuntimeError(error_message)
        return result.stdout.strip()
    except Exception as e:
        error_message = f"음성 파일 변환 중 오류 발생: {result.stderr}"
        raise RuntimeError(f"음성 파일 변환 중 오류 발생: {e}")
    finally:
        print("stt 수행완료")


def get_wav_file_from_s3(file_url):
    try:
        # 로컬 파일 경로 설정
        local_wav_path = os.path.join(
            os.path.dirname(__file__), "music", os.path.basename(file_url)
        )

        if file_url.startswith("s3://"):
            # S3 URL에서 버킷과 키를 추출
            bucket_name, key = parse_s3_url(file_url)
            s3_client.download_file(bucket_name, key, local_wav_path)
        else:
            # HTTPS URL인 경우 직접 다운로드
            download_file_from_https(file_url, local_wav_path)

        # 16kHz로 변환
        converted_wav_path = local_wav_path.replace(".wav", "_16k.wav")
        audio = AudioSegment.from_file(local_wav_path, format="wav")
        audio = audio.set_frame_rate(16000)
        audio.export(converted_wav_path, format="wav")

        return converted_wav_path
    except Exception as e:
        error_message = f"S3에서 파일을 가져오는 중 오류 발생: {e}"
        raise RuntimeError(error_message)


def download_file_from_https(url, local_path):
    import requests

    response = requests.get(url)
    response.raise_for_status()
    with open(local_path, "wb") as f:
        f.write(response.content)


def parse_s3_url(s3_url):
    # S3 URL을 파싱하여 버킷 이름과 키를 반환
    try:
        if not s3_url.startswith("s3://"):
            raise ValueError("올바른 S3 URL이 아닙니다.")
        parts = s3_url[5:].split("/", 1)
        if len(parts) != 2:
            raise ValueError("S3 URL 형식이 올바르지 않습니다.")
        return parts[0], parts[1]
    except Exception as e:
        error_message = f"S3 URL 파싱 중 오류 발생: {e}"
        raise RuntimeError(error_message)


def detect_inappropriate_content(text):
    # TODO: 부적절한 내용 탐지 로직 구현
    # inappropriate_keywords = ["금지된 단어1", "금지된 단어2"]
    # for keyword in inappropriate_keywords:
    #     if keyword in text:
    #         return True
    return False


def process_message(key, message):
    try:
        # 메시지에서 데이터를 추출
        file_url = message.get("fileUrl")
        original_text = message.get("content")

        # S3에서 파일 다운로드 및 변환
        wav_file_path = get_wav_file_from_s3(file_url)

        # TODO 노이즈 제거
        # STT 처리
        stt_text = stt(wav_file_path)
        print(f"STT 결과: {stt_text}")

        # 유사도 분석
        is_similar, similarity = check_similarity(original_text, stt_text)

        # 부적절한 내용 감지
        if detect_inappropriate_content(stt_text):
            analysis_result_status = "INCLUDE_INAPPROPRIATE_CONTENT"
        elif not is_similar:
            analysis_result_status = "NOT_READ_VOICE"
        else:
            analysis_result_status = "SUCCESS"

        # 결과 반환
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


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    consumer.subscribe([REQUEST_TOPIC])

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
                # print(headers)
                message_type = get_header(headers, "requiredResponseType")

                if message_type is None:  # validate
                    print("requiredResponseType header not found")

                print(f"Received message with key: {key}, value: {value}")

                response_key, response_value = process_message(key, value)
                print(
                    f"Sending response with key: {response_key}, value: {response_value}"
                )

                producer.produce(
                    RESULT_TOPIC,
                    key=response_key,
                    value=json.dumps(response_value),
                    headers=[("spring.kafka.type", message_type)],
                    callback=lambda err, msg: print(
                        f"Message delivery failed: {err}"
                        if err
                        else f"Message delivered to {msg.topic()} [{msg.partition()}]"
                    ),
                )
                producer.flush()
            except Exception as e:
                logger.error(f"메시지 처리 중 오류 발생: {e}")
                continue

    except KeyboardInterrupt:
        print("Shutting down consumer...")

    finally:
        consumer.close()


def get_header(headers, key):
    for header_key, header_value in headers:
        if header_key == key:
            return header_value.decode("utf-8")
    return None


if __name__ == "__main__":
    main()
