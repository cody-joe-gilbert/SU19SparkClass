from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField, SelectMultipleField, IntegerField
from wtforms.validators import DataRequired, Length, Email, EqualTo
from wtforms.fields import SelectField

class RegistrationForm(FlaskForm):
    STATE_CHOICES = [('AK', 'Alaska'),('AL', 'Alabama'), ('AR','Arkansas'),('AS', 'American Samoa'),('AZ', 'Arizona'),('CA', 'California'),('CO', 'Colorado'),
        ('CT', 'Connecticut'),('DC', 'District of Columbia'),('DE', 'Delaware'),('FL', 'Florida'),('GA', 'Georgia'),('GU', 'Guam'),('HI', 'Hawaii'),('IA', 'Iowa'),('ID', 'Idaho'),('IL', 'Illinois'),
        ('IN', 'Indiana'),('KS', 'Kansas'),('KY', 'Kentucky'),('LA', 'Louisiana'),('MA', 'Massachusetts'),('MD', 'Maryland'),('ME', 'Maine'),
        ('MI', 'Michigan'),('MN', 'Minnesota'),('MO', 'Missouri'),('MP', 'Northern Mariana Islands'),('MS', 'Mississippi'),('MT', 'Montana'),
        ('NA', 'National'),('NC', 'North Carolina'),('ND', 'North Dakota'), ('NE', 'Nebraska'),('NH', 'New Hampshire'),('NJ', 'New Jersey'),('NM', 'New Mexico'),
        ('NV', 'Nevada'),('NY', 'New York'),('OH', 'Ohio'),('OK', 'Oklahoma'),('OR', 'Oregon'),('PA', 'Pennsylvania'),('PR', 'Puerto Rico'),
        ('RI', 'Rhode Island'),('SC', 'South Carolina'),('SD', 'South Dakota'),('TN', 'Tennessee'),('TX', 'Texas'),('UT', 'Utah'),('VA', 'Virginia'),
        ('VI', 'Virgin Islands'),('VT', 'Vermont'),('WA', 'Washington'),('WI', 'Wisconsin'),('WV', 'West Virginia'), ('WY', 'Wyoming')]

    state = SelectField(label='State', validators=[DataRequired()], choices=STATE_CHOICES)

    income = IntegerField('Income', validators=[DataRequired()])
    
    gender = SelectField(u'Gender', validators=[DataRequired()], 
                                choices=[('f', 'Female'), ('m', 'Male'), ('na', 'Not Applicable')])

    race = SelectField(u'Race', validators=[DataRequired()], 
                                choices=[('nativeAmerican', 'American Indian or Alaska Native'), ('asian', 'Asian'),
                                ('africanAmerican' ,'Black or African American'), ('pacificIslander', 'Native Hawaiian or Other Pacific Islander'), 
                                ('white', 'White'), ('na', 'Not Applicable')])
    
    ethnicity = SelectField(u'Ethnicity', validators=[DataRequired()], 
                                choices=[('hispanic', 'Hispanic or Latino'), ('notHispanic', 'Not Hispanic or Latino'),
                                ('na', 'Not Applicable')])

    """
    TODO
    delete email field if not using database
    """
    email = StringField('Email', validators=[DataRequired(), Email()])

    submit = SubmitField('Submit')
