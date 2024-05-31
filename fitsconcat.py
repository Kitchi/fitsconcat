#! /usr/bin/env python

import os
import glob
import numpy as np

from multiprocessing.pool import Pool
from astropy.io import fits

def make_empty_image(imlist, nstokes=4, outname='concat.fits', mode="normal"):
    """
    Generate an empty dummy FITS data cube. The FITS cube can exceed available
    RAM.

    The 2D image dimensions are derived from the first cube in the list.
    The number of channels in the output cube is assumed to be the length
    of the input list of images.

    """

    with fits.open(imlist[0], memmap=True) as hud:
        xdim, ydim = np.squeeze(hud[0].data).shape[-2:]

    print("X-dimension: ", xdim)
    print("Y-dimension: ", ydim)

    zdim = int(len(imlist))
    wdim = nstokes

    dims = tuple([xdim, ydim, zdim, wdim])

    # create header

    dummy_dims = tuple(1 for d in dims)
    dummy_data = np.zeros(dummy_dims, dtype=np.float32)
    hdu = fits.PrimaryHDU(data=dummy_data)

    header = hdu.header
    for i, dim in enumerate(dims, 1):
        header["NAXIS%d" % i] = dim
        header["CRPIX1"] = int(xdim/2)
        header["CRPIX2"] = int(ydim/2)

    header.tofile(outname, overwrite=True)

    # create full-sized zero image
    header_size = len(
        header.tostring()
    )  # Probably 2880. We don't pad the header any more; it's just the bare minimum
    data_size = np.product(dims) * np.dtype(np.float32).itemsize
    # This is not documented in the example, but appears to be Astropy's default behaviour
    # Pad the total file size to a multiple of the header block size
    block_size = 2880
    data_size = block_size * (((data_size -1) // block_size) + 1)

    with open(outname, "rb+") as f:
        f.seek(header_size + data_size - 1)
        f.write(b"\0")


def update_fits_header(cube_path, header_dict):
    with fits.open(cube_path, memmap=True, ignore_missing_end=True, mode="update") as hud:
        header = hud[0].header
        for key, value in header_dict.items():
            header[key] = value



def fill_cube_with_images(imlist, nstokes=4, outname='concat.fits', mode="normal"):
    """
    Fills the empty data cube with fits data.

    The number of channels in the output cube is assumed to be the length
    of the input list of images.
    """
    # TODO: debug: if ignore_missing_end is False, throws an error
    outhdu = fits.open(outname, memmap=True, ignore_missing_end=True, mode="update")
    outdata = outhdu[0].data

    max_chan =  int(len(imlist))
    for ii in range(0, max_chan):
        print(f"Processing channel {ii}/{max_chan}", end='\r')
        with fits.open(imlist[ii], memmap=True) as hdu:
            for ss in range(nstokes):
                outdata[ss, ii, :, :] = hdu[0].data[ss, 0, :, :]

    outhdu.close()
    highest_channel = int(outdata.shape[1] + 1)
    fitsheader = {
            "CRPIX3": 1, #lowestChanNo,
            "NAXIS3": highest_channel,
            "CTYPE3": ("FREQ", ""),
            }

    update_fits_header(outname, fitsheader)


def insert_channel(channo, outname, imlist, max_chan, nstokes):
    # TODO: debug: if ignore_missing_end is False, throws an error
    print(f"Processing channel {channo}/{max_chan}", end='\r')

    outhdu = fits.open(outname, memmap=True, ignore_missing_end=True, mode="update")
    outdata = outhdu[0].data
    with fits.open(imlist[channo], memmap=True) as hdu:
        for ss in range(nstokes):
            outdata[ss, channo, :, :] = hdu[0].data[ss, 0, :, :]

    outhdu.close()


def fill_cube_with_images_multiprocess(imlist, nstokes=4, outname='concat.fits', mode="normal"):
    """
    Fills the empty data cube with fits data.

    The number of channels in the output cube is assumed to be the length
    of the input list of images.
    """


    pool = Pool()
    max_chan =  int(len(imlist))


    sol = [pool.apply_async(insert_channel, (chan,), {'outname':outname, 'imlist':imlist, 'max_chan':max_chan, 'nstokes':nstokes}) for chan in range(max_chan)]
    #pool.apply_async((insert_channel, range(max_chan), outname=outname, imlist=imlist)
    pool.close()
    pool.join()


    for s in sol:
        s.get()

    fitsheader = {
            "CRPIX3": 1, #lowestChanNo,
            "NAXIS3": max_chan,
            "CTYPE3": ("FREQ", ""),
            }

    update_fits_header(outname, fitsheader)




if __name__ == '__main__':
    imlist = sorted(glob.glob("XMMLSS_12_9p5_1624760792_J0228-0406_r0.0.chan???.image.fits"))

    make_empty_image(imlist)
    #fill_cube_with_images(imlist)
    fill_cube_with_images_multiprocess(imlist)
