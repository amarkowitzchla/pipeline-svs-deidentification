import logging
import os
from pathlib import Path

import pandas as pd
from tqdm import tqdm
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tifffile import TiffFile, TiffFrame, TiffPage


# image deid packages
import struct
import shutil
import re
import threading
import copy
import warnings


logger = logging.getLogger('idcprep')






# @@@@@@@@@@@@@@@@@@@@@@@  METADATA DEIDENTIFICATION @@@@@@@@@@@@@@@@@@@@@@@@@

def validate_deidentify_metadata_all(manifest_path:str):
    """ Validation deidentification of multiple WSIs listed in a CSV file.

        Export validation results to a CSV file in the same directory of the
        manifest file labeled 'validation_results.csv'.

        Parameters
        ----------
            manifest_path : str
                Path to csv file containing a column labeled 'location' and a
                list of WSI file paths 
    
    """

    assert manifest_path.endswith('.csv'), f'Must provide path to a csv file'

    export_dir = os.path.dirname(manifest_path)
    export_path = os.path.join(export_dir, 'validation_results.csv')

    manifest_df = pd.read_csv(manifest_path)
    locations = manifest_df['location'].dropna().to_list()

    all_reports = []

    for path in tqdm(locations, desc=f'Validating deidentification'):
        report = validate_deidentify_metadata(path)
        all_reports.append(report)


    tdf = pd.DataFrame(all_reports)
   
    tdf.to_csv(export_path, index=False)
    print(f'Exported validation results to {export_path}')

def validate_deidentify_metadata(svs_path:str, validate_mode:bool=False, verbose_validation:bool=False) -> dict[str,str|bool]:
    """ Checks whether slide label and macro images exist and whether 
        Filename and ImageID metadata are identical

        Parameters
        ----------
            svs_path : str
                Path to svs file

            verbose : bool, default=False
                Print validation results. If 1+ elements aren't deidentified, will print those element
                names, otherwise will say all are deidentified. 

        Returns
        -------
            validation_report : dict
                Map deidentification elements to bool status on whether it is deidentified
                * `path`: path to svs file being evaluated
                * `clean_filename`: bool whether the Filename matches the ImageID
                * `no_label`: bool whether the label image
                * `no_macro`: bool whether the macro image exists

    
    """
    validation_report = {
        'path':svs_path,
        'clean_filename': False,
        'no_label': True,
        'no_macro': True
    }


    from tifffile import TiffFile

    with open(svs_path, 'r+b') as fp:
        t = TiffFile(fp)
        
        # like in svs-deidentifier, will check for label and micro images through page count
        # for GT450 
        gt_check = _gt450_image_check(t)
        if isinstance(gt_check, dict):
            validation_report.update(gt_check)

        for page in t.pages:
            # filename deid validation
            clean_filename = screen_filename(page, validation_mode=validate_mode)

            if clean_filename:
                validation_report['clean_filename'] = True

            # image deid validation
            if 'label' in page.description: # type: ignore
                validation_report['no_label'] = False

            if 'macro' in page.description: # type: ignore
                validation_report['no_macro'] = False
        t.close()


    # ? very nested verbose printing logic, primarily because I wanted to log everything regardless
    # ? despite potentially making verbose printing obsolete
    if validate_mode:
        if all(i is True for i in validation_report.values() if not isinstance(i, str)):
            logger.info(f"{validation_report['path']} is completely deidentified")
            if verbose_validation:
                print(f"{validation_report['path']} is completely deidentified")
        else:
            logger.info(f"{validation_report['path']} contains identifiable elements:")
            if verbose_validation:
                print(f"{validation_report['path']} contains identifiable elements:")
                
            for element,status in validation_report.items():
                if status is False:
                    logger.info(f"\t- {element.split('_')[-1]}")
                    if verbose_validation:
                        print(f"\t- {element.split('_')[-1]}")

    return validation_report

def screen_filename(page: "TiffPage | TiffFrame", validation_mode: bool = False):
    """ Checks the Filename metadata element within the page description against the 
        ImageID metadata element. If not using for validation, will overwrite Filename
        metadata.

        Parameters
        ----------
            page : TiffPage
                Single layer in TIFF hiearchy being evaluated

            validation_mode : bool, default=False
                If true, will not overwrite metadata. Instead returns a boolean for 
                validation report generation.

    """

    # ignore pages without the Filename in the description
    if 'Filename' not in page.description: # type: ignore
        return

    # restructure description string to get Filename and ImageID metadata as key-value pairs
    descr_parts = page.description.split('|') # type: ignore
    fn_and_imageid = [i for i in descr_parts if 'Filename = ' in i or 'ImageID = ' in i]      
    
    # ensures additional metadata was not unknowingly extracted 
    if len(fn_and_imageid) > 2:
        raise ValueError(f'More than 2 keys identified with "Filename = " and "ImageID = ": {fn_and_imageid}')
    
    # reformat into dict map for easier value comparison
    kvp = {k:v for k,v in [tuple(i.split(' = ')) for i in fn_and_imageid]}
    
    # Filename should be identical to ImageID
    # explictly calling keys also ensures the correct key-value pairs were extracted
    if kvp['Filename'] == kvp['ImageID']:
        #! can print, but will do so twice for each image since filename is encoded in two layers 
        # print(f'Filename and ImageID are identical for {kvp['ImageID']}.svs')
        return True

    if validation_mode:
        # ! Simply flag the image for (repeat) deidentification and subsequent validation
        # ! If the flagged image has been assessed by this function before, flags for manual review
        return False

    # create new map with updated Filename
    # original map will be used to identify string within description for replacement
    updated_kvp = {i:kvp['ImageID'] for i in kvp.keys()}

    # reformat to match description string format
    # order of elements (Filename, ImageID) are preserved in kvp 
    updated_format = [f'{k} = {v}' for k, v in updated_kvp.items()]

    # overwrite svs tag with updated description reflecting the new Filename
    page.tags['ImageDescription'].overwrite(page.description.replace(fn_and_imageid[0], updated_format[0])) # type: ignore
    # print(f'Updated Filename metadata to match the ImageID for _') # ! can print if want to track

def _gt450_image_check(tiff_file: "TiffFile"):
    """ Match svs-deidentifier implementation for checking for macro/label images in GT450 scanned images

    Parameters
    ----------
        tiff_file : TiffFile
            WSI opened as a .tiff

    Returns
    -------
        image_validation_report : dict
            Map image deidentification elements to bool status on whether it is deidentified
            * `no_label`: bool whether the label image
            * `no_macro`: bool whether the macro image exists

    
    """
    if not 'Aperio Leica Biosystems GT450' in tiff_file.pages[0].description: # type: ignore
        return
    
    gt450_validation_report = {
        'no_label': True,
        'no_macro': True
    }
    
    n_pages = len(tiff_file.pages)
    # GT450 WSIs should only have the 5 standard pages
    if n_pages > 5:
        # ensure whether both macro and label are present
        # e.g., if there are 6 pages unclear whether the 6th page is
        # the macro or label image, so just set both to False to mark
        # for deidentificaiton
        return {i:False for i in gt450_validation_report.keys()}

    return gt450_validation_report



# @@@@@@@@@@@@@@@@@@@@@@@  IMAGE DEIDENTIFICATION @@@@@@@@@@@@@@@@@@@@@@@@@

# delete_associated_image will remove a label or macro image from an SVS file
def delete_associated_image(slide_path, image_type):
    from tifffile import TiffFile
    # THIS WILL ONLY WORK FOR STRIPED IMAGES CURRENTLY, NOT TILED

    allowed_image_types=['label','macro'];
    if image_type not in allowed_image_types:
        raise Exception('Invalid image type requested for deletion')

    fp = open(slide_path, 'r+b')
    t = TiffFile(fp)

    # logic here will depend on file type. AT2 and older SVS files have "label" and "macro"
    # strings in the page descriptions, which identifies the relevant pages to modify.
    # in contrast, the GT450 scanner creates svs files which do not have this, but the label
    # and macro images are always the last two pages and are striped, not tiled.
    # The header of the first page will contain a description that indicates which file type it is
    first_page=t.pages[0]
    filtered_pages=[]
    if 'Aperio Image Library' in first_page.description: # type: ignore
        filtered_pages = [page for page in t.pages if image_type in page.description] # type: ignore
    elif 'Aperio Leica Biosystems GT450' in first_page.description: # type: ignore
        if image_type=='label':
            filtered_pages=[t.pages[-2]]
        else:
            filtered_pages=[t.pages[-1]]
    else:
        # default to old-style labeled pages
        filtered_pages = [page for page in t.pages if image_type in page.description] # type: ignore

    num_results = len(filtered_pages)
    if num_results > 1:
        raise Exception(f'Invalid SVS format: duplicate associated {image_type} images found')
    if num_results == 0:
        #No image of this type in the WSI file; no need to delete it
        return

    # At this point, exactly 1 image has been identified to remove
    page = filtered_pages[0]

    # get the list of IFDs for the various pages
    offsetformat = t.tiff.offsetformat
    offsetsize = t.tiff.offsetsize
    tagnoformat = t.tiff.tagnoformat
    tagnosize = t.tiff.tagnosize
    tagsize = t.tiff.tagsize
    unpack = struct.unpack

    # start by saving this page's IFD offset
    ifds = [{'this': p.offset} for p in t.pages]
    # now add the next page's location and offset to that pointer
    for p in ifds:
        # move to the start of this page
        fp.seek(p['this'])
        # read the number of tags in this page
        (num_tags,) = unpack(tagnoformat, fp.read(tagnosize))

        # move forward past the tag defintions
        fp.seek(num_tags*tagsize, 1)
        # add the current location as the offset to the IFD of the next page
        p['next_ifd_offset'] = fp.tell()
        # read and save the value of the offset to the next page
        (p['next_ifd_value'],) = unpack(offsetformat, fp.read(offsetsize))

    # filter out the entry corresponding to the desired page to remove
    pageifd = [i for i in ifds if i['this'] == page.offset][0]
    # find the page pointing to this one in the IFD list
    previfd = [i for i in ifds if i['next_ifd_value'] == page.offset]
    # check for errors
    if(len(previfd) == 0):
        raise Exception('No page points to this one')
        return
    else:
        previfd = previfd[0]

    # get the strip offsets and byte counts
    offsets = page.tags['StripOffsets'].value
    bytecounts = page.tags['StripByteCounts'].value 

    # iterate over the strips and erase the data
    # print('Deleting pixel data from image strips')
    for (o, b) in zip(offsets, bytecounts):
        fp.seek(o)
        fp.write(b'\0'*b)

    # iterate over all tags and erase values if necessary
    # print('Deleting tag values')
    for key, tag in page.tags.items():
        fp.seek(tag.valueoffset)
        fp.write(b'\0'*tag.count)

    offsetsize = t.tiff.offsetsize
    offsetformat = t.tiff.offsetformat
    pagebytes = (pageifd['next_ifd_offset']-pageifd['this'])+offsetsize

    # next, zero out the data in this page's header
    # print('Deleting page header')
    fp.seek(pageifd['this'])
    fp.write(b'\0'*pagebytes)

    # finally, point the previous page's IFD to this one's IFD instead
    # this will make it not show up the next time the file is opened
    fp.seek(previfd['next_ifd_offset'])
    fp.write(struct.pack(offsetformat, pageifd['next_ifd_value']))

    fp.close()

# CopyOp: thread-safe file info to share data between copy and progress threads
class CopyOp(object):
    def __init__(self, start = []):
        self.lock = threading.Lock()
        self.value = start
        self.original = start
    def update(self, index, val):
        self.lock.acquire()
        try:
            for key, value in val.items():
                self.value[index][key]=value
        finally:
            self.lock.release()
    def read(self):
        self.lock.acquire()
        cp = copy.deepcopy(self.value)
        self.lock.release()
        return cp

# copy_and_strip_all: iterate over all files and copy and remove labels
def copy_and_strip_all(files,copyop:CopyOp):
    [copy_and_strip(file,copyop,index) for index,file in enumerate(files)]

# copy_and_strip: single file copy/deidentify operation. 
#   to be done in a thread for concurrent I/O using CopyOp object for progress updates 
def copy_and_strip(file: dict[str, str], copyop: CopyOp, index: int):
    """
    


    Parameters
    ----------
    file : dict[str,str]
        map of source path to destination path
    copyop : CopyOp
        _description_
    index : _type_
        _description_
    """
    # clean the paths of improper file separators for the OS 
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        oldname=os.path.sep.join(re.split('[\\\/]', file['source'])) # type: ignore
        newname=os.path.sep.join(re.split('[\\\/]', file['dest']))   # type: ignore

    # print(f'{oldname = }\n{newname = }')

    # remove the filename leaving just the path
    dest_path = os.path.sep.join(newname.split(os.path.sep)[:-1])
    try:
        # create the destination directory if necessary
        os.makedirs(dest_path, exist_ok=True)
        filename, file_extension = os.path.splitext(newname)
        # if filename.endswith('failme'):
            # raise ValueError('Cannot copy this file')
        # now the directory exists; check if the file already exists
        if not os.path.exists(newname):  # folder exists, file does not
            copyop.update(index, {'dest':newname})
            shutil.copyfile(oldname, newname)
        else:  # folder exists, file exists as well
            ii = 1
            # filename, file_extension = os.path.splitext(newname)
            while True:
                test_newname = f'{filename}({str(ii)}){file_extension}'
                if not os.path.exists(test_newname):
                    newname = test_newname
                    copyop.update(index, {'dest':newname, 'renamed':True})
                    shutil.copyfile(oldname, newname)
                    break 
                ii += 1
    
        logger.info("Deidentifying file.")
        delete_associated_image(newname,'label')
        delete_associated_image(newname,'macro')
        logger.info("Deidentification complete.")
    except Exception as e:
        try:
            os.remove(newname)
        except FileNotFoundError:
            pass
        finally:
            copyop.update(index, {'failed':True,'failure_message':f'{e}'})
            logger.error("Deidentification failed; removed copied file.")
    finally:
        copyop.update(index, {'done':True})
    return

def do_copy_and_strip(files:list[dict[str,str]]):
    """ Run svs-deidentifier do_copy_and_strip

        Parameters
        ----------
            files : list, length=n_wsi
                List of dictionaries with 2 keys for each WSI file:
                * `source` : path of original file location
                * `dest` : path to desired file location for copied file 
    
    """
    copyop = CopyOp([{'source':f['source'],
                      'dest':None,
                      'filesize':os.stat(f['source']).st_size,
                      'done':False,
                      'renamed':False,
                      'failed':False,
                      'failure_message':''} for f in files])

    # threading.Thread(target=track_copy_progress, args=[copyop]).start()            
    # threading.Thread(target=copy_and_strip_all, args=[files, copyop]).start()
    # for index, f in enumerate(files):
    #     threading.Thread(target=copy_and_strip, args=[f, copyop, index]).start()

    copy_and_strip_all(files, copyop)
    return


def format_deid_input(original_path:str, output_dir:str|Path):
    """ Format list of dictionaries for svs deidentification. 

        Parameters
        ----------
            data : pd.DataFrame
                Assumes data has already been filtered for invalid file locations.
    
        Returns
        -------
            svs_deid_input : list, length=n_wsi
                List of dictionaries with 2 keys for each WSI file:
                * `source` : path of original file location
                * `dest` : path to desired file location for copied file 

    """
    filename = os.path.basename(original_path)

    dest_path = os.path.join(output_dir, f'deid_{filename}') # should still have .svs file extension

    return [{
        'source': original_path,
        'dest': dest_path
    }]


def run_svs_deidentifier(
    source_dest_csv: Path,
    *,
    fail_fast: bool = False,
) -> list[dict[str, str]]:
    df = pd.read_csv(source_dest_csv)
    if set(df.columns) != {"source", "destination"}:
        raise ValueError('CSV must contain "source" and "destination" columns.')

    results: list[dict[str, str]] = []
    for _, row in df.iterrows():
        file_info = {"source": row["source"], "dest": row["destination"]}
        try:
            filesize = os.stat(file_info["source"]).st_size
            copyop = CopyOp(
                [
                    {
                        "source": file_info["source"],
                        "dest": None,
                        "filesize": filesize,
                        "done": False,
                        "renamed": False,
                        "failed": False,
                        "failure_message": "",
                    }
                ]
            )
            copy_and_strip(file_info, copyop, 0)
            status = copyop.read()[0]
            if status.get("failed") and fail_fast:
                raise RuntimeError("Deidentification failed with fail-fast enabled.")
            results.append(
                {
                    "destination": status.get("dest") or file_info["dest"],
                    "status": "success" if not status.get("failed") else "failed",
                    "error": status.get("failure_message", ""),
                }
            )
        except Exception as exc:
            if fail_fast:
                raise
            results.append(
                {
                    "destination": file_info["dest"],
                    "status": "failed",
                    "error": str(exc),
                }
            )

    return results


def deidentify_one(
    source: str,
    destination: str,
    *,
    fail_fast: bool = False,
) -> dict[str, str]:
    try:
        filesize = os.stat(source).st_size
        copyop = CopyOp(
            [
                {
                    "source": source,
                    "dest": None,
                    "filesize": filesize,
                    "done": False,
                    "renamed": False,
                    "failed": False,
                    "failure_message": "",
                }
            ]
        )
        copy_and_strip({"source": source, "dest": destination}, copyop, 0)
        status = copyop.read()[0]
        if status.get("failed") and fail_fast:
            raise RuntimeError("Deidentification failed with fail-fast enabled.")
        return {
            "destination": status.get("dest") or destination,
            "status": "success" if not status.get("failed") else "failed",
            "error": status.get("failure_message", ""),
        }
    except Exception as exc:
        if fail_fast:
            raise
        return {
            "destination": destination,
            "status": "failed",
            "error": str(exc),
        }



