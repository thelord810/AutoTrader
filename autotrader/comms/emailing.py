import os
import pandas as pd
import smtplib, ssl
from datetime import datetime
from email.mime.text import MIMEText
from autotrader.brokers.trading import Order
from email.mime.multipart import MIMEMultipart


def send_order(order: Order, mailing_list: dict, host_email: dict) -> None:
    """Send order summary from oanda."""
    instrument = order.instrument
    order_type = order.order_type
    size = order.size
    price = order.order_price
    stop_loss = order.stop_loss
    take_profit = order.take_profit
    strategy_name = order.strategy

    if instrument[-3:] == "JPY":
        val = 2
    else:
        val = 4

    if stop_loss is not None:
        stop_pips = round(abs(price - stop_loss) * 10**val, 1)
        stop_loss = round(stop_loss, val + 1)
    else:
        stop_pips = None

    if take_profit is not None:
        take_pips = round(abs(price - take_profit) * 10**val, 1)
        take_profit = round(take_profit, val + 1)
    else:
        take_pips = None

    # Email configuration settings
    sender_email = host_email["email"]
    password = host_email["password"]

    for person in mailing_list:

        last_name = person.split("_")[1]
        title = mailing_list[person]["title"]
        receiver_email = mailing_list[person]["email"]

        # Constuct message details
        time = datetime.now().strftime("%H:%M:%S")
        message = MIMEMultipart("alternative")
        message["Subject"] = "{0} market order placed at {1}".format(instrument, time)
        message["From"] = sender_email

        # Create the plain-text version of email
        plaintext = ""

        # Load HTML version of email
        file_dir = os.path.dirname(os.path.abspath(__file__))
        filename = "{}_{}_{}.html".format(instrument, size, datetime.now().timestamp())
        email_message_path = os.path.join(file_dir, filename)

        # Write email in html
        with open(email_message_path, "w+") as f:
            f.write("<p>Dear {} {},</p>\n".format(title, last_name))
            f.write("<p>A {} order has been placed for {} ".format(order_type, size))
            f.write("units of {} following an entry signal ".format(instrument))
            f.write("recieved by {}.</p>\n".format(strategy_name))

            f.write("<p>A summary of the entry signal is provided below.</p>\n")

            f.write('<table border="1">\n')
            f.write("<tbody>\n")
            f.write("<tr>\n")
            f.write("<td>Pair</td>\n")
            f.write("<td>{}</td>\n".format(instrument))
            f.write("</tr>\n")
            f.write("<tr>\n")
            f.write("<td>Size</td>\n")
            f.write("<td>{}</td>\n".format(size))
            f.write("</tr>\n")
            f.write("<tr>\n")
            f.write("<td>Price</td>\n")
            f.write("<td>{}</td>\n".format(price))
            f.write("</tr>\n")
            f.write("<tr>\n")
            f.write("<td>Time</td>\n")
            f.write("<td>{}</td>\n".format(time))
            f.write("</tr>\n")
            f.write("<tr>\n")
            f.write("<td>Stop loss</td>\n")
            f.write("<td>{0} ({1})</td>\n".format(stop_loss, stop_pips))
            f.write("</tr>\n")
            f.write("<tr>\n")
            f.write("<td>Take profit</td>\n")
            f.write("<td>{0} ({1})</td>\n".format(take_profit, take_pips))
            f.write("</tr>\n")
            f.write("</tbody>\n")
            f.write("</table>\n")

            f.write("<hr />\n")

        email_body = open(email_message_path, "r").read()
        html = email_body

        # Convert messages into plain/html MIMEText objects
        part1 = MIMEText(plaintext, "plain")
        part2 = MIMEText(html, "html")

        # Add HTML/plain-text parts to MIMEMultipart message
        message.attach(part1)
        message.attach(part2)

        # Create secure connection with server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)

            server.sendmail(sender_email, receiver_email, message.as_string())

        # Delete html file
        os.remove(email_message_path)


def send_order_summary(filepath: str, mailing_list: dict, host_email: dict) -> None:
    """Send summary of orders placed with AutoTrader."""

    file_dir = os.path.dirname(os.path.abspath(__file__))
    order_history = pd.read_csv(filepath, index_col=0, skipinitialspace=True)
    now = datetime.now()
    date = now.strftime("%B %d, %Y")

    # Email configuration settings
    sender_email = host_email["email"]
    password = host_email["password"]

    for person in mailing_list:
        last_name = person.split("_")[1]
        title = mailing_list[person]["title"]
        receiver_email = mailing_list[person]["email"]

        # Constuct message details
        message = MIMEMultipart("alternative")
        message["Subject"] = "AutoTrader Order Summary for {}".format(date)
        message["From"] = sender_email

        # Create the plain-text version of email
        plaintext = ""

        # Create HTML email
        email_message_path = os.path.join(file_dir, "order_summary.html")

        # Write email in html
        with open(email_message_path, "w+") as f:

            f.write("<p>Dear {} {},</p>\n".format(title, last_name))

            f.write("<p>Please find below a summary of orders placed on \n")
            f.write("your behalf by AutoTrader.</p>\n")

            f.write('<table border="1">\n')
            f.write("<tbody>\n")
            f.write("<tr>\n")
            f.write("<td>Order Time</td>\n")
            f.write("<td>Strategy</td>\n")
            # f.write('<td>Order Type</td>\n')
            f.write("<td>Granularity</td>\n")
            f.write("<td>Instrument</td>\n")
            f.write("<td>Signal Price</td>\n")
            f.write("<td>Size</td>\n")
            f.write("<td>Stop Loss</td>\n")
            f.write("<td>Take Profit</td>\n")
            f.write("</tr>\n")

            for index, row in order_history.iterrows():
                f.write("<tr>\n")
                f.write("<td>{}</td>\n".format(index))
                f.write("<td>{}</td>\n".format(row.strategy))
                # f.write('<td>{}</td>\n'.format(row.order_type))
                f.write("<td>{}</td>\n".format(row.granularity))
                f.write(
                    "<td>{}/{}</td>\n".format(row.instrument[:3], row.instrument[-3:])
                )
                f.write("<td>{}</td>\n".format(row.trigger_price))
                f.write("<td>{}</td>\n".format(row.order_size))
                f.write("<td>{}</td>\n".format(round(row.stop_loss, 5)))
                f.write("<td>{}</td>\n".format(round(row.take_profit, 5)))
                f.write("</tr>\n")

            f.write("</tbody>")
            f.write("</table>")

            # f.write('<p>&nbsp;</p>\n')
            f.write("<p>All the best in your trading endeavours,\n")
            f.write("<br />AutoTrader</p>\n")

        # Read file in
        email_body = open(email_message_path, "r").read()
        html = email_body

        # Convert messages into plain/html MIMEText objects
        part1 = MIMEText(plaintext, "plain")
        part2 = MIMEText(html, "html")

        # Add HTML/plain-text parts to MIMEMultipart message
        message.attach(part1)
        message.attach(part2)

        # Create secure connection with server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)

            server.sendmail(sender_email, receiver_email, message.as_string())

        # Delete html file and order history file
        os.remove(email_message_path)
        os.remove(filepath)


def send_scan_results(
    scan_results: dict, scan_details: dict, mailing_list: dict, host_email: dict
) -> None:
    """Send results of market scan."""
    time = datetime.now().strftime("%H:%M:%S")
    index = scan_details["index"]
    strategy_name = scan_details["strategy"]

    # Email configuration settings
    sender_email = host_email["email"]
    password = host_email["password"]

    for person in mailing_list:
        # first_name      = person.split('_')[0]
        last_name = person.split("_")[1]
        title = mailing_list[person]["title"]
        receiver_email = mailing_list[person]["email"]

        # Constuct message details
        message = MIMEMultipart("alternative")
        message["Subject"] = "Scan Results for {} at {}".format(strategy_name, time)
        message["From"] = sender_email

        # Create the plain-text version of email
        plaintext = ""

        # Load HTML version of email
        file_dir = os.path.dirname(os.path.abspath(__file__))
        # file_dir = "/home/kieran/Documents/AT/development/AutoTrader/emailing/"
        email_message_path = os.path.join(file_dir, "scan_results.html")

        # Write email in html
        with open(email_message_path, "w+") as f:
            if len(scan_results) > 0:
                f.write("<p>Dear {} {},</p>\n".format(title, last_name))
                f.write("<p>This is an automated message to notify you of \n")
                f.write("a recent match in a market scan you are running. The \n")
                f.write("details of the scan are as follows.\n")
                f.write("<br />Time of scan: {}.\n".format(time))
                f.write("<br />Scan strategy: {}.\n".format(strategy_name))
                f.write("<br />Scan index: {}.</p>\n".format(index))

                f.write(
                    "<p>The results from the scan are shown in the table below.</p>\n"
                )

                f.write('<table border="1">\n')
                f.write("<tbody>\n")
                f.write("<tr>\n")
                f.write("<td>Pair</td>\n")
                f.write("<td>Signal Price</td>\n")
                f.write("<td>Size</td>\n")
                f.write("<td>Stop Loss</td>\n")
                f.write("<td>Take Profit</td>\n")
                f.write("</tr>\n")

                for pair in scan_results:
                    size = scan_results[pair]["size"]
                    entry = scan_results[pair]["entry"]
                    stop = scan_results[pair]["stop"]
                    if stop is None:
                        stop = "None"
                    else:
                        stop = round(stop, 5)

                    take = scan_results[pair]["take"]
                    if take is None:
                        take = "None"
                    else:
                        take = round(take, 5)

                    signal = scan_results[pair]["signal"]

                    if size == 0:
                        size = "Long" if signal == 1 else "Short"

                    f.write("<tr>\n")
                    f.write("<td>{}</td>\n".format(pair))
                    f.write("<td>{}</td>\n".format(round(entry, 5)))
                    f.write("<td>{}</td>\n".format(size))
                    f.write("<td>{}</td>\n".format(stop))
                    f.write("<td>{}</td>\n".format(take))
                    f.write("</tr>\n")

                f.write("</tbody>")
                f.write("</table>")

                f.write("<p>&nbsp;</p>\n")
                f.write("<p>All the best in your trading endeavours,</p>\n")
                f.write("<p>AutoTrader</p>\n")
            else:
                f.write("<p>Dear {} {},</p>\n".format(title, last_name))
                f.write("<p>This is an automated message to notify you \n")
                f.write("that the scan which you are running is still operational.\n")
                f.write("The details of the scan are as follows.</p>\n")
                f.write("<p>Time of scan: {}.</p>\n".format(time))
                f.write("<p>Scan strategy: {}.</p>\n".format(strategy_name))
                f.write("<p>Scan index: {}.</p>\n".format(index))

                f.write("<p>All the best in your trading endeavours,\n")
                f.write("<br /><strong>AutoTrader</strong></p>\n")

        # Read file in
        email_body = open(email_message_path, "r").read()
        html = email_body

        # Convert messages into plain/html MIMEText objects
        part1 = MIMEText(plaintext, "plain")
        part2 = MIMEText(html, "html")

        # Add HTML/plain-text parts to MIMEMultipart message
        message.attach(part1)
        message.attach(part2)

        # Create secure connection with server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)

            server.sendmail(sender_email, receiver_email, message.as_string())

        # Delete html file
        os.remove(email_message_path)


def send_message(mailing_list: dict, host_email: dict, message: str) -> None:
    """A method to email a generic message.

    Parameters:
        mailing_list (dict): a dictionary containing email contacts.

        host_email (dict): a dictionary containing the account details of the
        host email account.

        message (str): the message to be sent.

    Refer to the AutoTrader documentation for more information:
    https://kieran-mackle.github.io/AutoTrader/docs/configuration-global#emailing
    """

    # Email configuration settings
    sender_email = host_email["email"]
    password = host_email["password"]

    for person in mailing_list:
        receiver_email = mailing_list[person]["email"]

        # Create secure connection with server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message)
