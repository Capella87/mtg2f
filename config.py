import logging
import sys

from pathlib import Path
from tomlkit import TOMLDocument, parse, dumps


logger = logging.getLogger(__name__)


class DefaultConfig:
    def __init__(self):
        basepath = Path(__file__).parent.resolve()

        self.mtg2 = {
            'executable': 'mtg2',
        }
        self.plink = {
            'version': '1.9',
            'executable': 'plink1.9' if sys.platform == 'linux' else 'plink.exe',
        }
        self.gcta = {
            'executable': 'gcta',
        }
        self.mtg2['path'] = str(basepath / 'dist' / self.mtg2['executable'])
        self.plink['path'] = str(basepath / 'dist' / self.plink['executable'])
        self.gcta['path'] = str(basepath / 'dist' / self.gcta['executable'])



def open_settings(path: Path) -> TOMLDocument:
    if not path.absolute().exists():
        logger.warning('Configuration file is not found at %s. Creating a new config.toml on %s', path, path)
        # Create a default config.toml file
        conf = DefaultConfig()
        configdoc = TOMLDocument()
        configdoc['mtg2'] = conf.mtg2
        configdoc['plink'] = conf.plink
        configdoc['gcta'] = conf.gcta

        logger.debug('Creating default configuration file at %s with content:\n%s', path, dumps(configdoc))
        with open(path, 'w', encoding='utf-8', newline='\n') as f:
            f.write(dumps(configdoc))
        return configdoc
    else:
        with open(path, 'r', encoding='utf-8') as f:
            config_content = f.read()
            configdoc = parse(config_content)
            return configdoc


def load_settings(path: Path | None = None) -> TOMLDocument:
    config = open_settings(path)
    print(config)
    return config


def write_settings(doc: TOMLDocument, path: Path) -> None:
    path = path.absolute()
    logger.debug('Writing configuration to %s with content:\n%s', path, dumps(doc))
    with open(path, 'w', encoding='utf-8', newline='\n') as f:
        f.write(dumps(doc))
