import subprocess


def main():
    # Start producer.py
    producer = subprocess.Popen(["python", "src/producer.py"], stdout=subprocess.PIPE)

    # Start process.py
    processor = subprocess.Popen(["python", "src/process.py"], stdout=subprocess.PIPE)

    # Communicate with subprocesses
    producer_output, producer_error = producer.communicate()
    processor_output, processor_error = processor.communicate()

    if producer_error:
        print(f"Error in producer.py: {producer_error}")
    if processor_error:
        print(f"Error in process.py: {processor_error}")


if __name__ == "__main__":
    main()
