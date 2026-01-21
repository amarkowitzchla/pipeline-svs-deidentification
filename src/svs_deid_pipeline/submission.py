import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import configure_openslide
from .utils import md5_checksum, load_esm_data


logger = logging.getLogger('idcprep')


@dataclass
class CCDIPathologyFileConstants:
    type:str='pathology_file'
    file_type:str='svs'
    file_mapping_level:str='sample'
    image_modality:str='Slide Microscopy'
    file_description:str='SVS formatted file of H&E-stained WSI'
    license:str='NA'
    deidentification_method:str='automatic'
    fixation_embedding_method:str='Formalin fixed paraffin embedded (FFPE)'

    def get_values(self):
        return self.__dict__
    

def _compare_metadata_headers(required:Iterable[str], current:Iterable[str]) -> tuple[list[str], int]:
    """ Compares collected metadata headers to whats needed 

        Parameters
        ----------
        required : Iterable[str]
            All metadata column headers in the CCDI submission template

        current : Iterable[str]
            All metadata column headers that have been collected, or of interest
            for comparison

        Returns
        -------
            missing_headers : list[str]
                Remaining headers needed for collection

            n : int
                Number of remaining headers needed for collection
    """
    missing = [h for h in required if h not in current]
    extra = [h for h in current if h not in required]

    if len(extra) > 0:
        logger.warning(f'\tIdentified {len(extra)} metadata values not found in requirements: {extra}')
        

    return missing, len(missing)


def generate_metadata_file_record(
    initial_info: pd.Series,
    updated_location: str,
    *,
    openslide_path: Path | None = None,
    esm_export_dir: Path | None = None,
):
    """ Generate single row (WSI) of CCDI metadata template
    
        Parameters
        ----------
            initial_info : pd.Series
                Known information from initial spreadsheet. Specifically, we need:
                * `rid` : used for sample ID
                * `specimen_id` : used to compare against ESM data to get extra info
                                  e.g., stain
                * `stain` : used if available
                * `file location` : used to map to ESM data, which uses original location

            updated_location : str
                Updated file location after copying during image deidentification. Used for the 
                actual metadata file instead of the original location
    
    """
    logger.info(f'Generating CCDI metadata record for {updated_location}')

    # try to fill missing stain data from ESM if it exists there
    if initial_info['stain'] == '' and esm_export_dir:
        initial_info['stain'] = _attempt_stain_retrieval(
            specimen_id=initial_info['specnum_formatted'],
            file_location=initial_info['location'],
            esm_export_dir=esm_export_dir,
        )
    # open updated location with openslide
    configure_openslide(openslide_path)
    from openslide import OpenSlide

    slide = OpenSlide(updated_location)
    file_image_id = slide.properties['aperio.Filename'] # post metadata deid, this should match image ID

    # starting point of record formation
    record = {
        'pathology_file_id': file_image_id,
        'file_url_in_cds': updated_location,
        'staining_method': initial_info['stain'],
        'sample.sample_id': initial_info['rid'],
        'file_name': f'{file_image_id}.svs'
    }

    # update with hard coded info across records
    record |= CCDIPathologyFileConstants().get_values()

    # update with info collected from file directory 
    record |= {
        'file_size': os.path.getsize(updated_location),
        'md5sum': md5_checksum(updated_location),
        'magnification': slide.properties['aperio.AppMag']
    }


    logger.info(f'Created record with {len(record)} elements')

    return record


def build_submission_dataframe(
    manifest_df: pd.DataFrame,
    source_dest_df: pd.DataFrame,
    *,
    openslide_path: Path | None = None,
    esm_export_dir: Path | None = None,
) -> pd.DataFrame:
    lookup = dict(zip(source_dest_df["source"], source_dest_df["destination"]))
    records: list[dict[str, str]] = []
    for _, row in manifest_df.iterrows():
        destination = lookup.get(str(row["location"]))
        if not destination:
            continue
        record = generate_metadata_file_record(
            row,
            destination,
            openslide_path=openslide_path,
            esm_export_dir=esm_export_dir,
        )
        records.append(record)
    return pd.DataFrame(records)


def write_submission_csv(submission_df: pd.DataFrame, out_dir: Path) -> Path:
    submission_dir = out_dir / "submission"
    submission_dir.mkdir(parents=True, exist_ok=True)
    submission_path = submission_dir / "submission.csv"
    submission_df.to_csv(submission_path, index=False)
    return submission_path


def _attempt_stain_retrieval(
    specimen_id: str,
    file_location: str,
    esm_export_dir: Path,
):
    """Looks for stain info in ESM data if missing"""
    esm_data = load_esm_data(esm_export_dir)

    if esm_data.empty:
        return ''
    
    if specimen_id in esm_data['Specimen Acc#'].to_list():
        subset = esm_data[esm_data['Specimen Acc#']==specimen_id]
        if file_location in subset['File Location'].to_list():
            return subset[subset['File Location']==file_location]['Stain'].values[0]

    return ''
    






