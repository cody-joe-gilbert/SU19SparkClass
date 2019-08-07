"""
CSCI-GA.3033-001: Big Data Application Development
Team Project Code
Cody Gilbert, Fang Han, Jeremy Lao

This script forms the entry point to the Flask web application.
To start the Flask application, ensure Flask is installed in your
environment and execute
`python run.py`
or add "run.py" as your system FLASK_APP variable and execute
`flask run`
Once executed, the web application can be accessed from a local web browser at
http://127.0.0.1:5000/

@author Fang Han
"""

from src.entry import app

if __name__ == '__main__':
    app.run(debug=True)
