
const {logger} = require("./middleware/logger");
const nodemailer = require("nodemailer");

function getTransporter() {
    return nodemailer.createTransport({
        host: 'mail.eprojecttrackers.com',
        port: 465,
        secureConnection: true,
        auth: {
            user: process.env.EMAIL_USER,
            pass: process.env.EMAIL_PASSWORD
        },
        tls: {
            secureProtocol: "TLSv1_method"
        }
    });
}

function getMailOptions(email, subject, message) {
    return {
        from: process.env.EMAIL_USER,
        to: email,
        bcc: 'vee.huen.phan@ericsson.com',
        subject: subject,
        text: message
    };
}

const sendEmail = async function (email, subject, message) {
    logger.info("Sending email to " + email);
    logger.info(email);
    logger.info(message) ;

    const transporter = getTransporter();
    const mailOptions = getMailOptions(email, subject, message);
    await transporter.sendMail(mailOptions, function (error, info) {
        if (error) {
            logger.error(error.message);
            return;
        }
        logger.error('Email sent: ' + info.response);
    });
};

module.exports = {
    sendEmail: sendEmail
};