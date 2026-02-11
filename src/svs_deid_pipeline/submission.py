import logging
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import configure_openslide
from .utils import md5_checksum, load_esm_data


logger = logging.getLogger('idcprep')

    
@dataclass
class CCDIPathologyMetadataFile:
    SUBMISSION_TEMPLATE_TYPE:str = 'ccdi-dcc'
    SUBMISSION_TEMPLATE_VERSION:str = "1.0.0"
    type: str = "pathology_file"
    sample_id: str = ""
    pathology_file_id: str = ""
    file_name: str = ""
    data_category: str = "Pathology Imaging"
    file_type: str = "svs"
    file_description:str = ""            # optional, not used -- exists for compatibility
    file_size: int = 0
    md5sum: str = ""
    file_mapping_level: str = "sample"
    file_access : str = "Open"
    acl: str = ""                        # not required if file_access=Open
    authz: str = "['/open']"
    file_url: str = ""
    dcf_indexd_guid: str = ""
    image_modality: str = "Slide Microscopy"
    license: str = "CC by 4.0"           # Arbitrarily set for now
    magnification: str|float = ""
    fixation_embedding_method: str = "Formalin fixed paraffin embedded (FFPE)"
    staining_method:str = ""
    deidentification_method:str = "automatic"
    slim_url:str = ""                    # optional, not used -- exists for compatibility
    crdc_rd:str = ""                     # optional, not used -- exists for compatibility
    guid:str = ""                        # optional, not used -- exists for compatibility
    sample_guid:str = ""                 # optional, not used -- exists for compatibility

    def __repr__(self):
        return f'{self.SUBMISSION_TEMPLATE_TYPE} v{self.SUBMISSION_TEMPLATE_VERSION} submission template for {self.sample_id or self.pathology_file_id}'

    def get_template_metadata(self):
        """View submission template type and version"""
        return {i:j for i,j in self.__dict__.items() if i.startswith('SUBMISSION_TEMPLATE_')}

    def update_record(self, data:dict):
        valid_keys = set(self.__dict__.keys())
        proposed_keys = set(data.keys())
        difference = proposed_keys.difference(valid_keys)
        
        assert len(difference) == 0, f'Supplying invalid keys: {sorted(difference)}'
        self.__dict__.update(data)
        return

    def get_formatted_record(self):
        """ Get the values used for submission and formats the sample_id 
            header while maintaining key-value pair order
        """        
        formatted = {}
        for i,j in self.__dict__.items():
            # ignore metadata values
            if i.startswith('SUBMISSION_TEMPLATE_'):
                continue
            # unfortunately, only sample_id requires formatting due to dot format
            elif i == 'sample_id':
                formatted[f'sample.{i}'] = j
            else:
                formatted[i] = j
        
        return formatted

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
                Known information from manifest spreadsheet. Specifically, we need:
                * `sample_id` : used for sample.sample_id
                * `specnum_formatted` : specimen ID used to compare against ESM data to 
                                        get extra info (e.g., stain). Only used if 
                                        `esm_export_dir` is provided.
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

    # initialize single row of the CCDI-DCC (v1.0.0) metadata template
    # with hardcoded constants
    record = CCDIPathologyMetadataFile()
    record.update_record({
        'pathology_file_id': file_image_id,
        'file_url': updated_location,
        'staining_method': initial_info['stain'],
        'sample_id': initial_info['sample_id'],
        'file_name': f'{file_image_id}.svs',
        'file_size': os.path.getsize(updated_location),
        'md5sum': md5_checksum(updated_location),
        'magnification': slide.properties['aperio.AppMag']
    })

    logger.info(f'Created record with {record}')

    return record.get_formatted_record()


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
    






