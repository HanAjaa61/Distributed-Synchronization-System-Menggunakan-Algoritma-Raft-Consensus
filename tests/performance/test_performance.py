import time
import threading
import requests

BASE_URL = "http://localhost:8000"

def send_request():
    try:
        response = requests.get(f"{BASE_URL}/health")
        assert response.status_code == 200
    except Exception as e:
        print("Error:", e)

def test_response_time():
    start = time.time()
    response = requests.get(f"{BASE_URL}/health")
    end = time.time()

    assert response.status_code == 200
    print("Response time:", end - start)
    assert (end - start) < 1.5  # max 1.5 detik

def test_concurrent_requests():
    threads = []

    for _ in range(20):  # 20 concurrent users
        t = threading.Thread(target=send_request)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("Concurrent test done")