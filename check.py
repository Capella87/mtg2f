import pathlib
import zipfile
import logging
import platform
import sys
import shutil
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


def _get_base_directory() -> Path:
    """Get the directory for storing downloaded files.

    In PyInstaller, __file__ points to a temporary directory.
    Use the directory containing the executable instead.
    """
    if getattr(sys, 'frozen', False):
        # Running as PyInstaller bundle
        return Path(sys.executable).parent
    else:
        # Running as normal Python script
        return Path(__file__).parent.resolve()


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

    plink_path = check_plink()
    if not plink_path:
        logging.error('plink is not found. Installing plink...')
        plink_path = install_plink()

    if plink_path:
        rt['plink'] = plink_path
    else:
        logging.error('plink is not found and the installation failed.')
        raise RuntimeError('plink is not found and the installation failed.')

    return rt

def check_plink() -> str | None:
    # 2. Try to type "plink" command
    plink_path = shutil.which('plink')
    if plink_path:
        logging.debug('plink is found at %s from PATH.', plink_path)
        return plink_path
    logging.debug('plink is not found on PATH.')

    # 3. Try to find plink file on the executable file's folder
    plink_path = _get_base_directory() / 'plink' if platform.system() != 'Windows' else _get_base_directory() / 'plink.exe'
    if plink_path.exists() and plink_path.is_file():
        logging.debug('plink is found at %s from the app folder.', plink_path)
        return str(plink_path)
    logging.debug('plink is not found on the app folder.')

    return None

def install_plink(url: str | None = None, custom_path: str | None = None) -> str | None:
    if not url:
        os_env = platform.system()
        if os_env == 'Windows':
            url = 'https://s3.amazonaws.com/plink1-assets/plink_win64_20250819.zip'
        elif os_env == 'Linux':
            url = 'https://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20250819.zip'
        elif os_env == 'Darwin':
            url = 'https://s3.amazonaws.com/plink1-assets/plink_mac_20250819.zip'
        else:
            logging.error('Unsupported operating system or environment: %s', os_env)
            return None
    try:
        session = _get_session_with_retries()

        logging.info('Downloading plink from %s ...', url)
        file_response = session.get(url, stream=True, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0'
        },
        timeout=100)
        logging.debug('Request headers: %s', file_response.request.headers)
        file_response.raise_for_status()
        basedir = _get_base_directory() if not custom_path else pathlib.Path(custom_path).parent.resolve()
        download_path = basedir / 'plink.zip' if not custom_path else custom_path
        with open(download_path, 'wb') as f:
            f.write(file_response.content)
        logging.info('plink zip file is downloaded.')
    except requests.exceptions.Timeout:
        logging.error('Download request time out. try again later, or download manually from %s', url)
        return None
    except requests.exceptions.RequestException as e:
        logging.error('Error downloading plink: %s', e)
        return None

    # Uncompress the file
    # TODO: Error handling
    logging.info('Uncompressing plink zip file...')
    with zipfile.ZipFile(download_path, 'r') as zip_ref:
        zip_ref.extractall(basedir)
    download_path.unlink()
    logging.info('plink zip file is uncompressed and successfully configured.')

    return basedir / 'plink'

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

    # 3. Try to find mtg2 file on the executable file's folder
    mtg2_path = _get_base_directory() / 'mtg2'
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
        basedir = _get_base_directory() if not custom_path else pathlib.Path(custom_path).parent.resolve()
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
