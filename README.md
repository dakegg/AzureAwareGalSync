# AzureAwareGalSync

This is an updated version of the GalSync source which ships with FIM & MIM.  It is designed for use in environments where Azure AD Connect has been deployed and resolves race-condition issues related to the ExchangeLabs x500 address as well as updates the target address for contacts to use the @onmicrosoft.com address once the source mailbox has been migrated to Office 365.

The solution has the following features:

* Resolves the race-condition when GalSync removes ExchangeLabs x500 addresses then AAD Connect adds them back
* Selects the onmicrosoft.com address as the target (if exists) for any contacts when the source mailbox has been migrated to Office 365
* Sourcecode is in C# versus VB for easier maintenance 

Standard installation of GalSync will suffice, however the following changes must be made:

1) Create a boolean MV attribute for Person objects named "migrated" <-case sensitive
2) Create an IAF for User->Person for each MA that will be a source for GalSync, the IAF should be a mapping type Advanced with a Flow Rule Name "CalculateMigrated" <- case sensitive
   
   Datasource attributes should be msExchRecipientTypeDetails and msExchRemoteRecipientType, Metaverse attribute will be "migrated" 

* No quotes should be used for MV attributes or Flow Rule Names
