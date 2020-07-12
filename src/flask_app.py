import os

from flask import Flask, url_for, request, send_file
from markupsafe import escape

app = Flask(__name__)

from postgis_raster_bridge.config import apihost, base_path, image_result_path, working_directory
from postgis_raster_bridge import readDataByArea

if working_directory is None:
    working_directory = os.path.abspath('results')

@app.route('/')
def index():
    message = f"""
    \n\n

    template: {apihost}{base_path}?param=value

    \n\ntest: <a href>{apihost}{base_path}?xMin=16.821519&yMin=41.059118&xMax=16.903489&yMax=41.152986&types=roads&classes=motorway</a>

    \n\n
    """
    return message

@app.route(base_path)
def api_v1():
    print(request.args)
    kwargs_mapping = {
        "x_min": "xMin",
        "y_min": "yMin",
        "x_max": "xMax",
        "y_max": "yMax", 
        "types": "types",
        "classes": "classes",
        "cell_size": "cell_size"
    }
    kwargs = {}
    for argname in kwargs_mapping:
        kwargs[argname] = request.args.get(kwargs_mapping[argname])
    
    # Handle Lists
    list_args = ['types', 'classes']
    for _arg in list_args:
        _value = kwargs.get(_arg, None)
        if _value is not None:
            kwargs[_arg] = [str(x.strip()) for x in _value.strip().split(',')]
    
    # Handle Floats
    float_args = ['x_min', 'y_min', 'x_max', 'y_max', 'cell_size']
    for _arg in float_args:
        _value = kwargs.get(_arg, None)
        if _value is not None:
            kwargs[_arg] = float(_value)
        
    # Strip Nones
    for key in list(kwargs.keys()):
        if kwargs[key] is None:
            del kwargs[key]

    print(kwargs)
    return read_data(**kwargs)

@app.route(image_result_path+'/<filename>')
def api_v1_results(filename):
    print(filename)
    filepath = os.path.join(working_directory, filename)
    print(filepath, os.path.exists(filepath))
    return send_file(filepath, as_attachment=True)


with app.test_request_context():
    print("API is active at url:", url_for('api_v1'))