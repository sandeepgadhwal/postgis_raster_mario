import os

from flask import Flask, url_for, request, send_file, jsonify
from markupsafe import escape

app = Flask(__name__)

from postgis_raster_bridge.config import apihost, base_path, jobs_directory
from postgis_raster_bridge import readDataByArea

@app.route('/')
def index():
    test_url = f"{apihost}/{base_path}?latU=16.903489&lonU=41.05911&latD=16.821519&lonD=41.152986&cellSize=100&tipoDati=superficieEdificato"
    message = f"""
    <html>
        <body>
            <br/>
            <code>
                template: {apihost}{base_path}?param=value
            </code>
            <br/>
            <br/>
            <code>
                test: <a href="{test_url}" _target="">{test_url}</a>
            </code>
            <br/>
        </body>
    </html>
    """
    return message

@app.route(f"/{base_path}")
def api_v1():
    print(request.args)
    kwargs_mapping = {
        "latU": "latU",
        "lonU": "lonU",
        "latD": "latD",
        "lonD": "lonD",
        "tipoDati": "tipoDati",
        "cellSize": "cellSize"
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
    float_args = ['latU', 'lonU', 'latD', 'lonD', 'cellSize']
    for _arg in float_args:
        _value = kwargs.get(_arg, None)
        if _value is not None:
            kwargs[_arg] = float(_value)
        
    # Strip Nones
    for key in list(kwargs.keys()):
        if kwargs[key] is None:
            del kwargs[key]

    print(kwargs)
    return jsonify(readDataByArea(**kwargs))

@app.route(f"/{base_path}/<jobid>/<filename>")
def api_v1_results(jobid, filename):
    print(jobid, filename)
    filepath = os.path.join(jobs_directory, jobid, filename)
    print(filepath, os.path.exists(filepath))
    return send_file(filepath, as_attachment=True)


with app.test_request_context():
    print("API is active at url:", url_for('api_v1'))