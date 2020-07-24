
import os
import json
from matplotlib import pyplot as plt
import gdal
import numpy as np

color_array = [
    [0., 0., 0., 0.],
    [0., 0., 0., 1.],
    [1., 1., 1., 1.]
]
color_array = np.array(color_array)

def visualize_image(image_path, imsize=5):
    # Read Image
    ds = gdal.Open(image_path)
    n_bands = ds.RasterCount
    image_array = ds.ReadAsArray()
    if n_bands == 1:
        image_array = image_array[None]
    band_names = ['Single Band Raster']
    
    print(f"Found {n_bands} bands in raster {image_path}")
    
    # If Image has multiple bands read the band names from the supporting csv
    if n_bands > 1:
        raster_info_file = image_path + '.json'
        if os.path.exists(raster_info_file):
            with open(raster_info_file) as f:
                raster_info = json.load(f)
            band_names = []
            for row in raster_info['layers']:
                title = f"Layer: {row['layer_name']} | class: {row['class_name']} | n_features: {row['features']}"
                band_names.append(title)
        else:
            band_names = [f"band_{x}" for x in range(0, n_bands)]
    
    # Start Plotting
    fig, axs = plt.subplots(n_bands, figsize=[imsize, imsize*n_bands])
    for i in range(0, n_bands):
        if n_bands == 1:
            ax = axs
        else:
            ax = axs[i]
        nodataval = ds.GetRasterBand(i+1).GetNoDataValue()
        arr = image_array[i]
        
        _arr = np.zeros_like(arr)
        _arr[arr == nodataval] = 0
        _arr[arr == arr[arr!=nodataval].min()] = 1
        _arr[arr == arr[arr!=nodataval].max()] = 2

        _arr = color_array[_arr.flatten()].reshape((*_arr.shape, 4))
        ax.imshow(_arr[::-1])
        ax.set_title(band_names[i])