from flask_mail import Mail
from flask_wtf import CSRFProtect

mail = Mail()
csrf = CSRFProtect()