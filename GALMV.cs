using Microsoft.VisualBasic;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
//=================================================================
//   Copyright (c) Microsoft Corporation.  All rights reserved.
//=================================================================

using Microsoft.MetadirectoryServices;
//using Microsoft.MetadirectoryServices.LoggingCs;


namespace MIM.Sync.GALSyncCs
{

	public class MVSynchronizer : GALSyncCs.Synchronizer, IMVSynchronization
	{
        // dkegg - 17 Jan 2016 - declaring all customizer variables as global 
        bool useHubAndSpoke;
        string HubDomainName;
        string HubMAName;
        bool UseServiceRoutingAddressAsTarget;
        bool RetainExchangeLabsX500;

		//
		// AD_RDN_MAX_SIZE only counts for rdn value, not type
		//
		private const int AD_RDN_MAX_SIZE = 64;
		private const int RETRY_NUM_LIMIT = 1000;

		private const string RDN_TYPE = "CN=";
		public void Initialize()
		{
			//
			// Fetch configuration data before any other operation
			//
			GetConfigurationData();

		}

		public void Terminate()
		{
			//
			// We don't need to release any resources
			//
		}


		public void Provision(MVEntry mventry)
		{
			int i = 0;
			CSEntry MasterConnector = null;
			ConnectedMA MA = default(ConnectedMA);

            
			//LoggingCs.Log("Entering provisioning for " + mventry.ToString());

			foreach ( ConnectedMA ConnMA in mventry.ConnectedMAs) {
				//CSEntry csentry = default(CSEntry);
				foreach ( CSEntry cseentry in ConnMA.Connectors) {
					if (cseentry.ConnectionRule == RuleType.Projection) {
						MasterConnector = cseentry;
					}
				}
			}

			//
			// For every MA, try to add a csentry, if there is not already one
			//

			for (i = 0; i <= galMAs.Length - 1; i++) {
				MA = mventry.ConnectedMAs[galMAs[i].MAName];
				if (0 == MA.Connectors.Count) {
					//
					// If there were no connectors, then we are going to add one

                    AddOrRenameConnector(ref MA, ref galMAs[i], mventry);


				} else if (1 == MA.Connectors.Count) {
					//
					// If there is one connector,
					// - if it is the master object connector then it is ok.
					// - if it is a replica object, then check for rename.
					// - if it is a join object outside the Synchronization 
					//   OU, then it is a problem, log it
					//
					CSEntry csentry = MA.Connectors.ByIndex[0];
					if (IsInSynchronizationOU(csentry)) {
						AddOrRenameConnector(ref MA, ref galMAs[i], mventry, csentry);
					} else {
						if ((!object.ReferenceEquals(csentry, MasterConnector))) {
							//
							// This object has joined.
							//
							string LogString = "A contact for this object " + MasterConnector.ToString() + " called contact " + csentry.ToString() + " already exists in forest represented by MA " + MA.Name + ". If you would like to preserve this " + "contact and have us manage it, please " + "move the contact into Synchronization OU. " + "If you would like us to create " + "a new contact and manage it, " + "please delete this one.";

							//LoggingCs.Log(LogString);
						}
					}

				} else {
					//
					// We have more than one connectors undert the same MA,
					// print an error message.
					//
					CSEntry csentry = default(CSEntry);
					int index = 0;
					int countCsRemaining = 0;
					bool contactOutsideSyncOU = false;

					string LogString = "Multiple or outside-synchronizaiton-OU " + "connector(s) for the MV object " + MasterConnector.ToString() + "exist, they are: ";


					for (index = MA.Connectors.Count - 1; index >= 0; index += -1) {
						csentry = MA.Connectors.ByIndex[index];

						if (csentry.ConnectionRule == RuleType.Provisioning) {
							//LoggingCs.Log("Disconnecting provisioned " + csentry.ToString() );
							csentry.Deprovision();
						} else {
							countCsRemaining = countCsRemaining + 1;
							LogString = LogString + csentry.ToString() + " lives in forest connected by " + MA.Name + " ";
							if (!IsInSynchronizationOU(csentry) && csentry.ObjectType == CONTACT) {
								contactOutsideSyncOU = true;
							}
						}
					}

					//
					// If we end up with more than one connector, or
					// any contact outside synchronization OU,
					// we want to log a warning message
					//
					if ((countCsRemaining > 1) || (true == contactOutsideSyncOU)) {
						LogString = LogString + ". Please refer to documentation " + "to resolve the conflict.";
						//LoggingCs.Log(LogString);
					}
				}
			}

		}

		public bool DetermineMVDeletion(CSEntry csentry, MVEntry mventry)
		{
            bool ShouldDeleteFromMV = false;
			
			if (IsUnknownObjectType(csentry)) {
				LogAndThrowUnexpectedDataException("Unhandled object type in DetermineMVDeletion " + "called with csentry " + csentry.ToString() + " and  mventry " + mventry.ToString());
			}
			//
			// Delete the mv object iff the deleted object is the master object
			//
            // replaced Switch Statement with IFELSE tree for C# runtime variable checking
            if (csentry.ObjectType == USER || csentry.ObjectType == GROUP)
            {
                ShouldDeleteFromMV = true;
            }
            else if (csentry.ObjectType == CONTACT)
            {
                if ((GetContactType(csentry) == null))
                {
                    ShouldDeleteFromMV = false;
                }
                else
                {
                    ShouldDeleteFromMV = mventry.ObjectType.Equals(GetContactType(csentry));
                }
            }
            else
            {
                LogAndThrowUnexpectedDataException("Unhandled object type in DetermineMVDeletion " + "called with csentry " + csentry.ToString() + " and  mventry " + mventry.ToString());
            }

            return ShouldDeleteFromMV;
		}
		bool IMVSynchronization.ShouldDeleteFromMV(CSEntry csentry, MVEntry mventry)
		{
			return DetermineMVDeletion(csentry, mventry);
		}


		private void AddOrRenameConnector(ref ConnectedMA MA, ref GALMA MAConfig, MVEntry mventry, CSEntry csentry = null)
		{
			//
			// All objects are provisioned as contacts
			//
			string cn = null;
			int numberToAppend = 1;
			bool successful = false;
			bool extendedNameTried = false;
			string extendedName = null;
			string adminGroup = null;
			bool provisioningAdd = false;
			int cnLengthMax = 0;
			string validatedName = null;

			//
			// Add or Rename if only SynchronizationOU is defined
			//
			if ((MAConfig.SynchronizationOU == null) || MAConfig.SynchronizationOU.Equals("")) {
				return;
			}

			if (!mventry[COMMON_NAME].IsPresent || !mventry[MAIL_NICK_NAME].IsPresent || !mventry[TARGET_ADDRESS].IsPresent) {
				LogAndThrowUnexpectedDataException("Provisioning without cn, mailNickName or targetAddress");
			}

			if (null == csentry) {
				provisioningAdd = true;
			}

			cn = mventry[COMMON_NAME].Value.ToString();

			//
			// Active Directory does not distinguish CNs that differ only in use of diacritical marks (accents) etc.
			// whereas the sync service does. So force uniqueness by appending mailnickname to all CNs with extended
			// chars if doing so does not exceed CN max length.
			//
			IEnumerator cnEnum = cn.GetEnumerator();
			while (cnEnum.MoveNext()) {

               
                
                
				if (Strings.AscW(cnEnum.Current.ToString()) > 127 && cn.Length + mventry[MAIL_NICK_NAME].Value.ToString().Length + 2 + RDN_TYPE.Length < AD_RDN_MAX_SIZE) {
					cn = cn + "(" + mventry[MAIL_NICK_NAME].Value.ToString() + ")";
					break; // TODO: might not be correct. Was : Exit While
				}
			}

			do {
				try {
					//
					// Create a DN for the new object, need UPPER case "CN=..."
					//
					string rdn = RDN_TYPE + cn;
					ReferenceValue dn = MA.EscapeDNComponent(rdn).Concat(MAConfig.SynchronizationOU);

					if (rdn.Length > AD_RDN_MAX_SIZE + RDN_TYPE.Length) {
						LogAndThrowUnexpectedDataException("RDN too long: " + rdn);
					}

					if (csentry == null) {
						//
						// Try to add the object
						//
						//LoggingCs.Log("Adding " + dn.ToString());

                        csentry = ExchangeUtils.CreateMailEnabledContact(MA, dn, mventry[MAIL_NICK_NAME].Value.ToString(), mventry[TARGET_ADDRESS].Value.ToString());

						adminGroup = GetAdminGroup(csentry);
						if ((adminGroup != null)) {
							//
							// LegacyExhangeDN = adminGroup/cn=mailnickname-guid
							//
							validatedName = ValidateLegacyExhangeDN(mventry[MAIL_NICK_NAME].Value.ToCharArray());

							if ((validatedName == null)) {
								csentry[LEGACY_EXCHANGE_DN].Value = adminGroup + "/cn=" + System.Guid.NewGuid().ToString();
							} else {
								csentry[LEGACY_EXCHANGE_DN].Value = adminGroup + "/cn=" + validatedName + "-" + System.Guid.NewGuid().ToString();
							}

						}
					} else {
						//
						// Try to rename the object
						//
						if (!csentry.DN.Equals(dn)) {
							//LoggingCs.Log("Renaming " + dn.ToString());
							csentry.DN = dn;
						}
					}
					successful = true;

				} catch (MissingParentObjectException ex) {
					//
					// Typically the admin has to perform a full/delta import
					// on the target CD, or disable provisioning until all 
					// forests are imported.
					//
					//LoggingCs.Log("Target MA " + MA.Name + " is not imported yet. " + "Please disable provisioning until all forests " + "are imported.");
					throw ex;


				} catch (ObjectAlreadyExistsException ex) {
					//
					// If adding connector, throw away the instance to start over
					//
					if (provisioningAdd) {
						csentry = null;
					}

					//
					// There is a duplicate object in the target AD, 
					// change the cn accordingly to avoid conflict.
					//
					if (!extendedNameTried) {
						extendedNameTried = true;
						try {
							if (mventry[DEPARTMENT].IsPresent) {
								extendedName = mventry[DEPARTMENT].Value;
							}
						} catch (NoSuchAttributeInObjectTypeException ex2) {
						}
					}

					cn = null;
					if (extendedName != null) {
						cn = mventry[COMMON_NAME].Value + " (" + extendedName + ")";
						extendedName = null;

						if (cn.Length > AD_RDN_MAX_SIZE) {
							//
							// If too long, we'll try without it
							//
							cn = null;
						}
					}


					if (null == cn) {
						cn = mventry[COMMON_NAME].Value;

						//
						// To make sure that the number appended
						// will not be truncated. 
						// The 2 spaces reserved is for "()"
						//
						cnLengthMax = AD_RDN_MAX_SIZE - (numberToAppend.ToString().Length + 2);
						//
						// If it's too long, we are going to truncate the 
						// name and preserve the number appended.
						//
						if (cn.Length > cnLengthMax) {
							cn = cn.Substring(0, cnLengthMax);
						}

						cn = cn + "(" + numberToAppend.ToString() + ")";
						numberToAppend = numberToAppend + 1;

						if (numberToAppend > RETRY_NUM_LIMIT) {
							LogAndThrowUnexpectedDataException("Retry for " + mventry[COMMON_NAME].Value + " exceeds limit " + numberToAppend.ToString());
						}

					}

				}

			} while (!successful);

		}

	}

}
