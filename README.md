# AzureAwareGalSync

This is an updated version of the GalSync source which ships with FIM & MIM.  It is designed for use in environments where Azure AD Connect has been deployed and resolves race-condition issues related to the ExchangeLabs x500 address as well as updates the target address for contacts to use the @onmicrosoft.com address once the source mailbox has been migrated to Office 365.

The solution has the following features:

* Resolves the race-condition when GalSync removes ExchangeLabs x500 addresses then AAD Connect adds them back
* Selects the onmicrosoft.com address as the target (if exists) for any contacts when the source mailbox has been migrated to Office 365
* Sourcecode is in C# versus VB for easier maintenance 
