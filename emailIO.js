'use strict';

//
// emailIO.js
// Sending emails to users
//
// VDJServer Analysis Portal
// VDJ API Service
// https://vdjserver.org
//
// Copyright (C) 2020 The University of Texas Southwestern Medical Center
//
// Author: Scott Christley <scott.christley@utsouthwestern.edu>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
//

var emailSettings = require('./emailSettings');
var nodemailer    = require('nodemailer');
var sendmailTransport = require('nodemailer-sendmail-transport');
//var config = require('../config/config');
var webhookIO = require('./webhookIO');

var emailIO = {};
module.exports = emailIO;

// Tapis
var tapisSettings = require('vdj-tapis-js/tapisSettings');
var tapisIO = tapisSettings.get_default_tapis();
var config = tapisSettings.config;

var transporter = null;
if (!emailSettings.relayHost) {
    config.log.info('WARNING', 'emailSettings.relayHost is not defined, mail cannot be sent.');
} else {
    transporter = nodemailer.createTransport({ host: emailSettings.relayHost, port: emailSettings.relayPort, secure: false });
}

emailIO.sendPasswordResetEmail = async function(recipientEmail, username, passwordResetCode) {
    var context = 'emailIO.sendPasswordResetEmail';
    var msg = null;

    if (!transporter) {
        config.log.info(context, 'no mail transporter, ignoring send email request.', true);
        return Promise.resolve();
    }

    var passwordResetUrl = tapisSettings.vdjBackbone + '/password-reset/' + passwordResetCode;

    var mailOptions = {
        to: recipientEmail,
        from: emailSettings.fromAddress,
        replyTo: emailSettings.replyToAddress,
        subject: 'VDJServer Password Reset',
        generateTextFromHTML: true,
        html: 'A password reset request has been submitted to vdjserver.org.'
              + '<br>'
              + '<br>'
              + 'Reset your password for account "' + username + '" with reset code: ' + passwordResetCode + ', or by clicking on the link below:'
              + '<br>'
              + '<br>'
              + 'Please go to <a href="' + passwordResetUrl + '">' + passwordResetUrl + '</a> to reset your password.'
              + '<br>'
              + '<br>'
              + 'If you have not submitted a password reset request, then please disregard this email.',

    };

    var info = await transporter.sendMail(mailOptions)
        .catch(function(error) {
            msg = 'could not send email, error: ' + error;
        });
    if (msg) {
        msg = config.log.error(context, msg);
        webhookIO.postToSlack(msg);
    } else {
        config.log.info(context, 'email sent: ' + info.messageId);
    }

    return Promise.resolve();
};

emailIO.sendFeedbackEmail = async function(recipientEmail, feedback) {
    var context = 'emailIO.sendFeedbackEmail';
    var msg = null;

    if (!transporter) {
        config.log.info(context, 'no mail transporter, ignoring send email request.', true);
        return Promise.resolve();
    }

    var mailOptions = {
        to: recipientEmail,
        from: emailSettings.fromAddress,
        replyTo: emailSettings.replyToAddress,
        subject: 'VDJServer.org Feedback',
        text: feedback,
    };

    var info = await transporter.sendMail(mailOptions)
        .catch(function(error) {
            msg = 'could not send email, error: ' + error;
        });
    if (msg) {
        msg = config.log.error(context, msg);
        webhookIO.postToSlack(msg);
    } else {
        config.log.info(context, 'email sent: ' + info.messageId);
    }

    return Promise.resolve();
};

emailIO.sendFeedbackAcknowledgementEmail = async function(recipientEmail, feedback) {
    var context = 'emailIO.sendFeedbackAcknowledgementEmail';
    var msg = null;

    if (!transporter) {
        config.log.info(context, 'no mail transporter, ignoring send email request.', true);
        return Promise.resolve();
    }

    var mailOptions = {
        to: recipientEmail,
        from: emailSettings.fromAddress,
        replyTo: emailSettings.replyToAddress,
        subject: 'VDJServer.org Feedback',
        text: 'Thank you for your feedback! Someone will respond shortly. Your feedback text is shown below\n\n-----\n\n' + feedback,
    };

    var info = await transporter.sendMail(mailOptions)
        .catch(function(error) {
            msg = 'could not send email, error: ' + error;
        });
    if (msg) {
        msg = config.log.error(context, msg);
        webhookIO.postToSlack(msg);
    } else {
        config.log.info(context, 'email sent: ' + info.messageId);
    }

    return Promise.resolve();
};

emailIO.sendWelcomeEmail = async function(recipientEmail, username, verificationId) {
    var context = 'emailIO.sendWelcomeEmail';
    var msg = null;

    if (!transporter) {
        config.log.info(context, 'no mail transporter, ignoring send email request.', true);
        return Promise.resolve();
    }

    var vdjWebappUrl = tapisSettings.vdjBackbone
                     + '/account'
                     + '/verify'
                     + '/' + verificationId
                     ;

    var mailOptions = {
        to: recipientEmail,
        from: emailSettings.fromAddress,
        replyTo: emailSettings.replyToAddress,
        subject: 'VDJServer Account Verification',
        generateTextFromHTML: true,
        html: 'Welcome to VDJServer!'
              + '<br>'
              + '<br>'
              + 'Please verify your account "' + username + '" with verification code: ' + verificationId + ', or by clicking on the link below:'
              + '<br>'
              + '<br>'
              + '<a href="' + vdjWebappUrl + '">' + vdjWebappUrl + '</a>.'
    };

    var info = await transporter.sendMail(mailOptions)
        .catch(function(error) {
            msg = 'could not send email, error: ' + error;
        });
    if (msg) {
        msg = config.log.error(context, msg);
        webhookIO.postToSlack(msg);
    } else {
        config.log.info(context, 'email sent: ' + info.messageId);
    }

    return Promise.resolve();
};

emailIO.sendGenericEmail = async function(recipientEmail, subject, message) {
    var context = 'emailIO.sendGenericEmail';
    var msg = null;

    if (!transporter) {
        config.log.info(context, 'no mail transporter, ignoring send email request.', true);
        return Promise.resolve();
    }

    var mailOptions = {
        to: recipientEmail,
        from: emailSettings.fromAddress,
        replyTo: emailSettings.replyToAddress,
        subject: subject,
        generateTextFromHTML: true,
        html: message
    };

    var info = await transporter.sendMail(mailOptions)
        .catch(function(error) {
            msg = 'could not send email, error: ' + error;
        });
    if (msg) {
        msg = config.log.error(context, msg);
        webhookIO.postToSlack(msg);
    } else {
        config.log.info(context, 'email sent: ' + info.messageId);
    }

    return Promise.resolve();
};
