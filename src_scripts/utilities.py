# type: ignore

### Standard imports ###

import re
import os
import pickle
import logging

### Non-standard imports ###

import itertools
import numpy as np

from PIL import Image

def unpickler(path):

    """ Unpickle a log file containing several pickled objects.
    Returns a generator to iterate over.

    Arguments:
    ----------
    path = str or Path-like
        The absolute path to the log file.
    """

    try:

        with open(path, 'rb') as _log_:
            while True:
                try:
                    _obj_ = pickle.load(_log_)
                    yield _obj_
                except EOFError:
                    break
    except IOError:
         return None

def reader(path):

    """ Helpful generator to read a log file. 

    Arguments:
    ----------
    path = str or Path-like
        The absolute path to the log file.
    """

    try:
        with open(path, 'r') as _file_:
            for line in _file_:
                yield line
    except IOError:
        return None

def file_walker(top, predicates=[]):

    """ Generator that filters results of os.scandir using a set of predicate functions.

    Arguments:
    ----------
    top: str or Path-like
        The directory to scan for files.

    Keyword Arguments:
    ------------------
    predicates: function-like
        A function-like argument to use to filter the files.
    """

    with os.scandir(top) as files:
        for f in files:   
            cond = all(pred(top, f) for pred in predicates)
            if cond:
                yield f

def grouper(iterable, n):

    """ Iterate through iterable, yielding groups of n elements. The last group may have less
    than n elements.

    Arguments:
    ----------
    iterable: iterable-like
        The iterable to iterate through.
    n: int
        Number of elements to form groups of.
    """
    
    # we want izip_longest() in python 2.x
    # or zip_longest() in python 3.x

    if hasattr(itertools, 'zip_longest'):
        zipper = itertools.zip_longest
    else:
        zipper = itertools.izip_longest
    args = [iter(iterable)] * n
    for group in zipper(*args, fillvalue=None):
        filtered_group = [val for val in group if val is not None]
        yield filtered_group

def step_iter(sequence, vmin, vmax, step):

    """ Helper iterator, used to generate a list of steps (start and end value pairs)
    of a particular step length from a sequence of numbers.

    Arguments:
    -----------
    sequence: iterable-like
        The sequence of numbers from which the steps are generated.
        Can be any iterable, as long as it has numbers for elements.
    vmin: int or float
        Minimum value to be considered when making the steps.
    vmax: int or float
        Maximum value to be considered when making the steps.
    step: int or float
        The step length.
    """

    # Ignore steps outside of bounds

    array = np.asarray(sorted(list(sequence)))
    mask = (array >= vmin) & (array <= vmax)
    array = array[mask]
    
    # Yield values separated by at least 'step'

    last = None
    rtol = 1e-7 # Deal with float rounding errors
    for value in array:
        if last is None or value - last >= step * (1-rtol):
            last = value
            yield value

def filter_by_ext(folder, **kwargs):

    """ Generator that filters out files by extension using the "FileWalker" function.

    Arguments:
    -----------
    folder: str or Path-like
        The directory where the files are stored.

    Keyword Arguments:
    ------------------
    extension: str
        The extension to filter the files with.
    """

    predicate = [lambda x, y: os.path.join(x,y).endswith(kwargs['extension'])]
    return file_walker(folder, predicate)

def count_files(folder, **kwargs):

    """ Function that counts the number of files, filtered by extension 
    using the "filter_by_ext" function.

    Arguments:
    -----------
    folder: str or Path-like
        The directory where the files are stored.

    Keyword Arguments:
    ------------------
    extension: str
        The extension to filter the files with.
    """

    files = filter_by_ext(folder, extension = kwargs['extension'])
    count = len([1 for x in list(files) if x.is_file()])
    return count

def list_files(startpath):

    """ Function to print out the directory structure starting from the
    starting paths.
    
    Arguments:
    ----------
    startpath: str or Path-like
        The starting path of the directoryr tree.
    """

    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))    

def make_pdf(img_dir, save_to_pdf):

    """ Make a single, multi-page PDF document out of a directory of PNG
    images.

    Arguments:
    ----------
    img_dir: str or Path-like
        The directory that contains the images.
    save_to_pdf: str or Path-like
        The absolute path to the PDF document.
    """

    img_files = filter_by_ext(img_dir, extension = '.png')
    images = []

    for img_file in img_files:

        IMG = Image.open(img_file.path)
        if IMG.mode == 'RGBA':
            IMG = IMG.convert('RGB')
        images.append(IMG)

    images[0].save(save_to_pdf, save_all = True, quality=100, append_images = images[1:])
