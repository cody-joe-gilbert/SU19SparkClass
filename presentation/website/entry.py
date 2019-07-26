from flask import Flask, render_template, url_for, flash, redirect, request
from forms import RegistrationForm
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db'
db = SQLAlchemy(app)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    income = db.Column(db.Integer, unique=False, nullable=False)
    state = db.Column(db.String(20), unique=False, nullable=False)
    gender = db.Column(db.String(10), unique=False, nullable=False)
    ethnicity = db.Column(db.String(30), unique=False, nullable=False)
    race = db.Column(db.String(30), unique=False, nullable=False)
    email = db.Column(db.String(120), unique=False, nullable=False)

    def __repr__(self):
        return f"User('{self.income}', '{self.state}','{self.gender}', '{self.race}', '{self.ethninicity}', '{self.email}')"

db.create_all()
posts = [
    {
        'title': 'Read our paper: Using Big Data Systems to Analyze Big (not Jumbo) Mortgage Data',
        #'authors': 'Cody, Jeremy, Fang', 
        'content': 'Banking professionals are required to submit data to Federal regulators for the purposese of monitoring the health and safety of the financial system and individual banks. However, banks are also required by law to help promote growth in their local economies through lending. The Home Mortgage Disclosure acts requires banks and lenders to provide low-level mortgage application data to the Consumer Finance Protection Bureau (CFPB). Federal banking regulators analyze the data to discern economic trends and monitor for unfair lending practices. Our analysis utilizes big data architecture, namely Spark, to dig deeper into the numbers to analyze denial rates by race group, gender, and various borrower characteristics. Our visualization application will serve as a tool for both regulators and lenders to help identify possible red flags in lending practices.',
    },
    {
        #'author': '<delete>',
        'title': 'Learn more about HMDA data',
        'content': '<Link to HMDA website>',
    }
]

@app.route("/")
@app.route("/home")
def home():
    return render_template('home.html', posts=posts)


@app.route("/visualization")
def visualization():
    """
    TODO
    waiting on data
    """
    return render_template('HMDACounties.html', title='Visualization')


@app.route("/register", methods=['GET', 'POST'])
def register():
    form = RegistrationForm()
    if form.validate_on_submit():
        user = User(state=form.state.data, gender = form.gender.data, income=form.income.data, email=form.email.data, race=form.race.data, ethnicity=form.ethnicity.data)
        db.session.add(user)
        db.session.commit()
        flash(f'User information of {form.email.data} has been plugged into the model, generating result...', 'success')
        
        """
        TODO
        plug user info into model, whose result will be visualized in /modeling 
        """
        return redirect(url_for('modeling'))
    return render_template('findLender.html', title='Register', form=form)

@app.route("/mapping", methods=['GET'])
def mapping():
    return render_template('HMDACounties.html', title='Mapping')

@app.route("/modeling", methods=['GET', 'POST'])
def modeling():
    """
    TODO
    visualize modeling result
    add button to redirect to mapping
    """
    return render_template('modeling.html', title='Modeling')


if __name__ == '__main__':
    app.run(debug=True)
