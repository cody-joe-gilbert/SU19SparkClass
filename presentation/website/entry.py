from flask import Flask, render_template, url_for, flash, redirect, request
from forms import RegistrationForm
from flask_sqlalchemy import SQLAlchemy
from recommend import plotTopLenders
import logging
import logging.config

#CJG: Setup logger for debugging
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('entry')
logger.info('starting flask...')

# runSpark flag: If you have all the spark and PySpark directories setup,
#   set to True, otherwise False. If you set to True and don't have all the
#   spark configurations, then you will get many, many errors.
runSpark = True
if runSpark:
    from driverCode import runModel
    modeler = runModel() # Run early to let the SparkContext startup

app = Flask(__name__)
app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db'
db = SQLAlchemy(app)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    income = db.Column(db.Integer, unique=False, nullable=False)
    loanAmnt = db.Column(db.Integer, unique=False, nullable=False)
    state = db.Column(db.String(20), unique=False, nullable=False)
    gender = db.Column(db.String(10), unique=False, nullable=False)
    ethnicity = db.Column(db.String(30), unique=False, nullable=False)
    race = db.Column(db.String(30), unique=False, nullable=False)
    email = db.Column(db.String(120), unique=False, nullable=False)

    def __repr__(self):
        return f"User('{self.income}', '{self.state}','{self.gender}', '{self.race}', '{self.loanAmnt}', '{self.ethninicity}', '{self.email}')"

@app.route("/")
@app.route("/index")
def index():
    return render_template('index.html', title="Home")


@app.route("/visualizeByState")
def visualizeByState():
    return render_template('heatMap_slider.html', title='VisualizeByState')

@app.route("/visualizeByKeyParam")
def visualizeByKeyParam():
    return render_template('plotsByParam.html', title='VisualizeByKeyParam')

@app.route("/paper", methods=['GET'])
def paper():
    """
    TODO
    fix relative path of paper
    """
    return render_template('paper.html', title="Paper")

@app.route("/register", methods=['GET', 'POST'])
def register():
    form = RegistrationForm() 
    if form.validate_on_submit():
        logger.info('lender form submitted')
        #flash('We\'re working hard to find you some good lenders!')
        if runSpark:
            logger.info('running prediction modeling')
            modeler.runPrediction(form)
            visTable = modeler.predData  # Output for visualization
            #visTable.to_pickle("./dummy.pkl")
            visTable.to_json(path_or_buf="./tmp.json", orient='values')
            #plotter = plotTopLenders(visTable) # DIRECTLY READING FROM DF CAUSES INCONSISTENCY IN RECOMMEND.PY
            plotter = plotTopLenders() # TAKING A DETOUR, READING THE ./tmp.json IN RECOMMEND.PY
            plotter.plot()
            return redirect(url_for('register'))
    return render_template('findLender.html', title='Register', form=form)


if __name__ == '__main__':
    app.run(debug=True)
