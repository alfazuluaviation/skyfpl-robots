import subprocess
import os

def pull_mbtiles():
    src = "files/charts/enrc/ENRCL_BRASIL_FULL.mbtiles"
    dst = "ENRCL_METADATA_AUDIT.mbtiles"
    package = "com.skyfpl.app"
    
    cmd = ["adb", "exec-out", f"run-as {package} cat {src}"]
    
    print(f"Pulling {src} from device...")
    with open(dst, "wb") as f:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while True:
            chunk = process.stdout.read(1024 * 1024) # 1MB chunk
            if not chunk:
                break
            f.write(chunk)
            print(f"Downloaded {os.path.getsize(dst)} bytes", end="\r")
        
        stderr_data = process.stderr.read()
        if stderr_data:
            print(f"\nError: {stderr_data.decode()}")
        
        process.wait()
        if process.returncode == 0:
            print(f"\nSuccessfully pulled {dst} ({os.path.getsize(dst)} bytes)")
        else:
            print(f"\nProcess failed with return code {process.returncode}")

if __name__ == "__main__":
    pull_mbtiles()
