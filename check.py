import pathlib
import zipfile
import logging
import sys
import shutil
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


def check(custom_path: str | None = None) -> dict:
    rt = {}
    mtg2_path = check_mtg2(custom_path)
    if not mtg2_path:
        logging.info('mtg2 is not found. Installing mtg2...')
        mtg2_path = install_mtg2()

    if mtg2_path:
        rt['mtg2'] = mtg2_path
    else:
        logging.error('mtg2 is not found and the installation failed.')
        raise RuntimeError('mtg2 is not found and the installation failed.')

    return rt

def check_mtg2(custom_path: str | None = None) -> str | None:

    # Find custom_path (Includes binary) first
    if custom_path:
        custom_path_obj = Path(custom_path)
        if custom_path_obj.exists() and custom_path_obj.is_file():
            logging.debug('mtg2 is found on %s.', custom_path)
            return str(custom_path_obj.resolve())
        else:
            logging.debug('Custom mtg2 path %s not found.', custom_path)

    # 2. Try to type "mtg2" command
    mtg2_path = shutil.which('mtg2')
    if mtg2_path:
        logging.debug('mtg2 is found at %s from PATH.', mtg2_path)
        return mtg2_path
    logging.debug('mtg2 is not found on PATH.')

    # 3. Try to find ./mtg2 file on the app folder
    mtg2_path = Path('./mtg2').resolve()
    if mtg2_path.exists() and mtg2_path.is_file():
        logging.debug('mtg2 is found at %s from the app folder.', mtg2_path)
        return str(mtg2_path)
    logging.debug('mtg2 is not found on the app folder.')
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

        logging.info('Downloading mtg2 from %s ...', url)
        file_response = session.get(url, stream=True, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0'
        },
        timeout=100)
        logging.debug('Request headers: %s', file_response.request.headers)
        file_response.raise_for_status()
        basedir = pathlib.Path(__file__).parent.resolve() if not custom_path else pathlib.Path(custom_path).parent.resolve()
        download_path = basedir / 'mtg2.zip' if not custom_path else custom_path
        with open(download_path, 'wb') as f:
            f.write(file_response.content)
        logging.info('mtg2 zip file is downloaded.')
    except requests.exceptions.Timeout:
        logging.error('Download request time out. try again later, or download manually from %s', url)
        return None
    except requests.exceptions.RequestException as e:
        logging.error('Error downloading mtg2: %s', e)
        return None

    # Uncompress the file
    # TODO: Error handling
    logging.info('Uncompressing mtg2 zip file...')
    with zipfile.ZipFile(download_path, 'r') as zip_ref:
        zip_ref.extractall(basedir)
    download_path.unlink()
    logging.info('mtg2 zip file is uncompressed and successfully configured.')

    return basedir / 'mtg2'
