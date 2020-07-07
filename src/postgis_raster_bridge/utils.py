import requests
from tqdm import tqdm
from urllib import request
import shutil

def download_file(url, file_save_path):
    # print(f"-- Downloading {ownerhsip_data_path}")
    if "http://" in url or "https://" in url:
        with requests.get(url, allow_redirects=True, stream=True, verify=False) as r:

            total_size = int(r.headers.get('content-length', 0))
            block_size = 1024 * 1024 * 5  # 5 MB
            t = tqdm(total=total_size, unit='iB', unit_scale=True)

            with open(file_save_path, 'wb') as f:
                for data in r.iter_content(block_size):
                    t.update(len(data))
                    f.write(data)
    else:
        with request.urlopen(url) as r:
            try:
                total_size = int(r.headers.as_string().split('Content-length:')[-1].split()[0])
                print(f"-- FTP downloading {float(total_size / 1024 / 1024):.2f} MB file")
            except:
                pass
            with open(file_save_path, 'wb') as f:
                shutil.copyfileobj(r, f)
    print(f"-- Successfully downloaded {file_save_path}")