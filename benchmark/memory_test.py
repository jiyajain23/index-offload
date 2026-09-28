import time

chunks = []

for i in range(1, 101):
    chunk = bytearray(10 * 1024 * 1024)

    # Actually touch every page so the memory is committed.
    for j in range(0, len(chunk), 4096):
        chunk[j] = 1

    chunks.append(chunk)

    print(f"Actually using approximately {i * 10} MB", flush=True)
    time.sleep(0.2)

print("Finished allocation.", flush=True)
time.sleep(60)