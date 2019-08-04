from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField, SelectMultipleField, IntegerField
from wtforms.validators import DataRequired, Length, Email, EqualTo
from wtforms.fields import SelectField
from formsEntries import STATE_CHOICES, ETHNICITY, RACE, GENDER

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

    """
    TODO
    delete email field if not using database
    """
    email = StringField('Email', validators=[DataRequired(), Email()])

    submit = SubmitField('Submit')
