"""Sequence utilities.

Functions
---------
chunks
    Yield successive n-sized chunks from lst.
"""

def chunks(lst, n):
    """Yield successive n-sized chunks from lst.

    The last chunk may be shorter.

    Parameters
    ----------
    lst
        The sequence to split.
    n
        The chunk size.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
