from get_platform import operating_system

class EmailSender:
    """
    Client responsible for sending a publishing request to PST via outlook
    Will not attempt to send a request if not on a windows machine
    Uses windows api to send the email
    """
    def __init__(self, upload_dict):
        self.upload_dict = upload_dict
        self.email_to = "" # email recipient
        self.email_dict = {}
        self.create()

    def create(self):
        for dataset in self.upload_dict:
            email_subject = f"CMD publishing request - {dataset}"
            email_body = f"""
Please can you publish the following

Dataset: {dataset}
Collection: {self.upload_dict[dataset]["collection_name"]}
"""
            self.email_dict[dataset] = {
                "email_subject": email_subject,
                "email_body": email_body
            }

    def send(self):
        if operating_system != "windows":
            # not raising an error
            # will want the pipeline to continue
            print("***")
            print("Aborting email request - can only send a publishing request email on an ONS machine")
            print("Continuing pipeline")
            print("***")
            return

        import win32com.client as win32
        for dataset in self.email_dict:
            email_subject = self.email_dict[dataset]["email_subject"]
            email_body = self.email_dict[dataset]["email_body"]

            outlook = win32.Dispatch('outlook.application')
            email = outlook.CreateItem(0)
            email.To = self.email_to
            email.Subject = email_subject
            email.body = email_body
            email.Send()

            print(f"Publishing request email sent for {dataset}")
        







