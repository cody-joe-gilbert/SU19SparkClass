"""
CSCI-GA.3033-001: Big Data Application Development
Team Project Code
Cody Gilbert, Fan Han, Jeremy Lao

This script forms the entry point to the Flask web application.
To start the Flask application, ensure Flask is installed in your
environment and execute
`python entry.py`
or add "entry.py" as your system FLASK_APP variable and execute
`flask run`
Once executed, the web application can be accessed from a local web browser at
http://127.0.0.1:5000/

@author: Fang Han
with Editing and documentation by Cody Gilbert
"""
from flask import Flask, render_template, url_for, flash, redirect, request
from forms import RegistrationForm
from flask_sqlalchemy import SQLAlchemy
from recommend import plotTopLenders
from driverCode import runModel
import logging
import logging.config
import os

#Setup logger for debugging
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('entry')
logger.info('starting flask...')

# Start the SparkContext
modeler = runModel() 

# Start the Flask application
app = Flask(__name__)

# Configure and start the SQLAlchemy App for query logging
app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db'
db = SQLAlchemy(app)

# Define the user query logging DB ORM
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    income = db.Column(db.Integer, unique=False, nullable=False)
    loanAmnt = db.Column(db.Integer, unique=False, nullable=False)
    state = db.Column(db.String(20), unique=False, nullable=False)
    gender = db.Column(db.String(10), unique=False, nullable=False)
    ethnicity = db.Column(db.String(30), unique=False, nullable=False)
    race = db.Column(db.String(30), unique=False, nullable=False)
    #email = db.Column(db.String(120), unique=False, nullable=False)

    def __repr__(self):
        return f"User('{self.income}', '{self.state}','{self.gender}', '{self.race}', '{self.loanAmnt}', '{self.ethninicity}')"

# Define the home page/index
@app.route("/")
@app.route("/index")
def index():
    return render_template('index.html', title="Home")

# Define the applicant denial rate visual page
@app.route("/visualizeByState")
def visualizeByState():
    return render_template('heatMap_slider.html', title='VisualizeByState')

# Define the key parameter visualization page
@app.route("/visualizeByKeyParam")
def visualizeByKeyParam():
    return render_template('plotsByParam.html', title='VisualizeByKeyParam')

# Define the final paper repository page
@app.route("/paper", methods=['GET'])
def paper():
    """
    TODO
    fix relative path of paper
    """
    return render_template('paper.html', title="Paper")

# Define the Lender Recommendation Tool page
@app.route("/register", methods=['GET', 'POST'])
def register():
    form = RegistrationForm() 
    if form.validate_on_submit():
        logger.info('lender form submitted')
        
        # Create the approval probabilities from the model
        visTable = modeler.runPrediction(form)
        
        # Translate to temp json file; stopgap due to pandas DF parsing issue
        visTable.to_json(path_or_buf=os.path.join(os.getcwd(),'tmp.json'),
                         orient='values')
        
        # Plot the results to the page
        plotter = plotTopLenders() # TAKING A DETOUR, READING THE ./tmp.json IN RECOMMEND.PY
        plotter.plot()
        return redirect(url_for('register'))
    return render_template('findLender.html', title='Register', form=form)

if __name__ == '__main__':
    app.run(debug=True)
