import hashlib
import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger('idcprep')

# returns dataframe with empty file locations dropped
def read_and_extract_data(path: str | Path) -> pd.DataFrame:
    path = Path(path) if isinstance(path, str) else path

    if path.suffix == '.xlsx':
        df = pd.read_excel(path)
    elif path.suffix == '.csv':
        df = pd.read_csv(path)
    else:
        raise ValueError(f'Invalid file type supplied: {path.name}. Expected a .csv or .xlsx file')
    
    assert 'location' in df.columns, f'The provided file ({path}) is missing a "location" column. This column should contain file location paths'
    
    return df[df['location'].isin(df['location'].dropna())]

def format_output_path(output_dir: str | Path, original_path: str):
    filename = os.path.basename(original_path)

    return os.path.join(output_dir, f'deid_{filename}') # should still have .svs file extension


def calculate_ccdi_file_sizes(locations):
    """ Get file sizes of specified file locations

        Parameters
        ----------
            locations : pd.Series
                Series of file paths to calculate file sizes from

        Returns
        -------
            sizes : pd.Series
                Series of file sizes in bytes
    
    """
    abs_paths = locations.apply(os.path.abspath)

    sizes = abs_paths.apply(os.path.getsize)

    return sizes.sum() / 1_000_000_000

def md5_checksum(file_path: str, block_size: int = 2**20):
    """ Calculate MD5 checksum of a file

        Parameters
        ----------
            file_path : str
                Path to the file

            block_size : int, default=1 MB
                The size of chunks to read the file 

        Returns
        -------
            hash : str
                32-character hexadecimal MD5 checksum
    """
    m = hashlib.md5()
    try:
        with open(file_path, 'rb') as fn:
            while True:
                data = fn.read(block_size)
                if not data:
                    break
                m.update(data)
    except IOError:
        logger.warning("File could not be read for checksum.")
        return None
    return m.hexdigest()






def read_and_merge_data(dir: str | Path) -> pd.DataFrame:
    """ Retrieves piecemeal Aperio exports and merges into single dataframe
    
        Removes extra empty column artefact from Aperio exporting. Reformats manual
        stain entry marker (STAIN_) to all caps. 

        Reads all csvs in the provided directory

        Parameters
        ----------
            dir : str
                path to directory containing csv files of Aperio exports

        Returns
        -------
            esm_data : pd.DataFrame
                All exported ESM data -- unformatted (e.g., manual entries/markers such as stains)
    
    """
    assert os.path.isdir(dir), f'Path passed was not a directory: {dir}'
    
    dfs = [pd.read_csv(os.path.join(dir, i)) for i in os.listdir(dir)]
    logger.info(f'Read {len(dfs)} exported CSVs from {dir}')

    merged = pd.concat(dfs, ignore_index=True).infer_objects()
    merged['Comment.1'] = merged['Comment.1'].replace(regex='stain_', value='STAIN_')
    logger.info(f'Removed extra column and updated stain markers to "STAIN_"')
    return merged.drop(columns=['Unnamed: 11'])

def update_stain_info(data: pd.DataFrame):
    """Updates Stain values of provided dataframe in place according to comments"""
    cleaned_comments = data.dropna(subset='Comment.1').filter(['Stain', 'Comment.1'])
    
    # only get rows with a stain in the comments
    filtered_comments = cleaned_comments[cleaned_comments['Comment.1'].str.contains('STAIN_')]

    # override current stain entry with the comment version
    # typically a nan value, but sometimes populated with closest entry in ESM while comment
    # contains slightly more detail about the stain (e.g., NF entry but NF200 comment)
    resolved = filtered_comments['Comment.1'].map(_resolve_stain)

    # merge back with original dataframe
    data.update({'Stain':resolved}) # type:ignore

    logger.info(f'Successfully updated stains from manual entries (n={len(filtered_comments)})')

def _resolve_stain(value: str):
    # split comments containing stain info and other stuff (e.g. CCDI)
    stain_comment = [i for i in value.split(';') if 'STAIN_' in i][0]
    # split STAIN tag from the actual stain info
    stain_parts = stain_comment.split('_')
    # reformat comments with 2 stains, originally separated by a comma
    # to be separated by a semicolon 
    if stain_parts[0].startswith('2'):
        return ';'.join(stain_parts[1].split(','))
    else:
        return stain_parts[1]

# set up
def load_esm_data(export_dir: str | Path, debug: bool = False) -> pd.DataFrame:
    """Combines read/merge/stain update as general load data function.

    Parameters
    ----------
    export_dir : str | Path
        Directory containing exported ESM CSVs.
    debug : bool, default=False
        Log debugging for:
        - check for mistagged manually entered stains
    """
    directory = Path(export_dir)
    if not os.path.exists(directory):
        logger.warning(f'No ESM data detected')
        return pd.DataFrame()

    ramd = read_and_merge_data(directory)
    update_stain_info(ramd)

    if debug:
        logger.debug(f'== VIEWING COMMENTS FOR MANUALLY ENTERED STAINS MISSING THE TAG ==')
        # check for manually entered stains missing the tag
        misentered_stains = [i for i in ramd['Comment.1'].unique() if isinstance(i, str) and not i.startswith('STAIN_')]
        for comment in misentered_stains:
            logger.debug(f'\t- {comment}')


    return ramd


