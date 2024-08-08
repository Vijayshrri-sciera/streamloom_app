from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Email, Regexp

class LoginForm(FlaskForm):
    email = StringField('Email', validators=[
        DataRequired(),
        Email(),
        Regexp(r'^[a-zA-Z0-9_.+-]+@sciera\.com$', message='Email must be @sciera.com')
    ])
    password = PasswordField('Password', validators=[DataRequired()])
    submit = SubmitField('Login')
