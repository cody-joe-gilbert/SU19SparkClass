from flask import Flask, render_template, url_for, flash, redirect
from forms import RegistrationForm

app = Flask(__name__)
app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'

posts = [
    {
        'title': 'Read our paper: Using Big Data Systems to Analyze Big (not Jumbo) Mortgage Data',
        'authors': 'Cody, Jeremy, Fang', 
        'content': 'Banking professionals are required to submit data to Federal regulators for the purposese of monitoring the health and safety of the financial system and individual banks. However, banks are also required by law to help promote growth in their local economies through lending. The Home Mortgage Disclosure acts requires banks and lenders to provide low-level mortgage application data to the Consumer Finance Protection Bureau (CFPB). Federal banking regulators analyze the data to discern economic trends and monitor for unfair lending practices. Our analysis utilizes big data architecture, namely Spark, to dig deeper into the numbers to analyze denial rates by race group, gender, and various borrower characteristics. Our visualization application will serve as a tool for both regulators and lenders to help identify possible red flags in lending practices.',
    },
    {
        'author': '<delete>',
        'title': 'Learn more about HMDA data',
        'content': '<Link to HMDA website>',
    }
]


@app.route("/")
@app.route("/home")
def home():
    return render_template('home.html', posts=posts)


@app.route("/about")
def about():
    return render_template('about.html', title='About')


@app.route("/register", methods=['GET', 'POST'])
def register():
    form = RegistrationForm()
    if form.validate_on_submit():
        flash(f'User profile created for {form.email.data}!', 'success')
        return redirect(url_for('modeling'))
    return render_template('findLender.html', title='Register', form=form)


@app.route("/modeling", methods=['GET', 'POST'])
def modeling():

    return render_template('modeling.html', title='Modeling')


if __name__ == '__main__':
    app.run(debug=True)
