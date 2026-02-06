import pathlib
import zipfile
import sys
import shutil
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


def check() -> dict:
    rt = {}
    mtg2_path = check_mtg2()
    if not mtg2_path:
        print('mtg2 is not found. Installing mtg2...')
        mtg2_path = install_mtg2()

    if mtg2_path:
        rt['mtg2'] = mtg2_path
    else:
        raise RuntimeError('mtg2 is not found and installation failed.')

    return rt

def check_mtg2(custom_path: str | None = None) -> str | None:

    # Find custom_path (Includes binary) first
    if custom_path:
        custom_path_obj = Path(custom_path)
        if custom_path_obj.exists() and custom_path_obj.is_file():
            print(f'mtg2 is found on {custom_path}.')
            return str(custom_path_obj.resolve())
        else:
            print(f'Custom mtg2 path {custom_path} not found.')

    # 2. Try to type "mtg2" command
    mtg2_path = shutil.which('mtg2')
    if mtg2_path:
        print(f'mtg2 is found at {mtg2_path} from PATH.')
        return mtg2_path
    print('mtg2 is not found on PATH.')

    # 3. Try to find ./mtg2 file on the app folder
    mtg2_path = Path('./mtg2').resolve()
    if mtg2_path.exists() and mtg2_path.is_file():
        print(f'mtg2 is found at {mtg2_path} from the app folder.')
        return str(mtg2_path)
    print('mtg2 is not found on the app folder.')
    return None

def _get_session_with_retries(retries: int = 3, backoff_factor: float = .3) -> requests.Session:
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session

def install_mtg2(url: str | None = None, custom_path: str | None = None) -> Path | None:
    url = 'https://www.dropbox.com/s/1oqvssyewt43283/MTG2_v2.22.zip?dl=1'
    try:
        session = _get_session_with_retries()

        print(f'Downloading mtg2 from {url} ...')
        file_response = session.get(url, stream=True, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0'
        },
        timeout=100)
        file_response.raise_for_status()
        basedir = pathlib.Path(__file__).parent.resolve() if not custom_path else pathlib.Path(custom_path).parent.resolve()
        download_path = basedir / 'mtg2.zip' if not custom_path else custom_path
        with open(download_path, 'wb') as f:
            f.write(file_response.content)
        print(f'mtg2 zip file is downloaded.')
    except requests.exceptions.Timeout:
        print(f'Download request time out. try again later, or download manually from {url}')
        return None
    except requests.exceptions.RequestException as e:
        print(f'Error downloading mtg2: {e}', file=sys.stderr)
        return None

    # Uncompress the file
    # TODO: Error handling
    print('Uncompressing mtg2 zip file...')
    with zipfile.ZipFile(download_path, 'r') as zip_ref:
        zip_ref.extractall(basedir)
    download_path.unlink()
    print('mtg2 zip file is uncompressed and successfully configured.')

    return basedir / 'mtg2'
