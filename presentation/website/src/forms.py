"""
CSCI-GA.3033-001: Big Data Application Development
Team Project Code
Cody Gilbert, Fang Han, Jeremy Lao

This file contains the Flask self-validating form used for acquiring user input. 

@author: Fang Han
"""

from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField, SelectMultipleField, IntegerField
from wtforms.validators import DataRequired, Length, Email, EqualTo
from wtforms.fields import SelectField
from src.formsEntries import STATE_CHOICES, ETHNICITY, RACE, GENDER

class RegistrationForm(FlaskForm):
    
    state = SelectField(label='State', validators=[DataRequired()], choices=STATE_CHOICES)

    income = IntegerField('Income', validators=[DataRequired()])

    loanAmnt = IntegerField('Loan Amount', validators=[DataRequired()])

    gender = SelectField(u'Gender', validators=[DataRequired()],
                                choices=GENDER)

    race = SelectField(u'Race', validators=[DataRequired()],
                                choices=RACE)

    ethnicity = SelectField(u'Ethnicity', validators=[DataRequired()],
                                choices=ETHNICITY)

    # email = StringField('Email', validators=[DataRequired(), Email()])

    submit = SubmitField('Submit')
