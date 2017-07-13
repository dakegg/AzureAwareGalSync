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

	public class MASynchronizer : GALSyncCs.Synchronizer, IMASynchronization
	{


		public void Initialize()
		{
			//
			// Fetch configuration data for GalSync customizations
			//
			GetConfigurationData();

		}

		public void Terminate()
		{
			//
			// We don't need to release any resources
			//
		}

		public bool FilterForDisconnection(CSEntry csentry)
		{
            bool shouldFilterForDisconnection = false;

            if (csentry.ObjectType == USER)
            {
               shouldFilterForDisconnection =  FilterUserObjects(csentry);
            }
            else if (csentry.ObjectType == CONTACT)
            {
                shouldFilterForDisconnection = FilterContactObjects(csentry);
            }
            else if (csentry.ObjectType == GROUP || csentry.ObjectType == DYNAMICDDL)
            {
                shouldFilterForDisconnection =  FilterGroupObjects(csentry);
            }
            else
            {
                LogAndThrowUnexpectedDataException("Unhandled object type in FilterForDisconnection " + "called with entry " + csentry.ToString());
            }

            return shouldFilterForDisconnection;
		}


		public void MapAttributesForJoin(string FlowRuleName, CSEntry csentry, ref ValueCollection values)
		{
			//
			// Clear the values that are going to be used for join
			//
			values.Clear();

			switch (FlowRuleName) {
				case "ExtractX500ProxyAddresses":
					ExtractFromProxyAddresses(csentry, ref values, X500_PREFIX);

					break;
				default:
					LogAndThrowUnexpectedDataException("Unhandled FlowRuleName in MapAttributesForJoin " + "called with entry " + csentry.ToString() + " and flow rule name " + FlowRuleName);
					break;
			}
		}

		public bool ResolveJoinSearch(string joinCriteria, CSEntry csentry, MVEntry[] rgmventry, out int MVEntry, ref string MVObjectClass)
		{

			string exceptionString = null;
	
			string hasNonProvisionedConnector = null;

			//
			// If more than one candidate is present, it is an error
			//

			if (1 < rgmventry.Length) {
				int i = 0;
				exceptionString = "There are multiple objects representing " + "the same entity in the metaverse, they are: ";

				for (i = 0; i <= rgmventry.Length - 1; i++) {
					exceptionString = exceptionString + " " + MasterConnector(rgmventry[i]);
				}

				exceptionString = exceptionString + " We cannot join, please examine the object " + csentry.ToString() + "and the join rules and determine what attribute(s) " + "caused the conflict.  Please clean up the object to " + "allow it to be propagated to other forests if it is " + "authoritative or use Joiner to connect it to " + "an metaverse object to maintain it.";

				LogAndThrowUnexpectedDataException(exceptionString);
			}

			//
			// Do not want to join from the authoritative container even if
			// there is only one MV candidate.
			//

			if ((csentry.ObjectType.Equals(CONTACT) && IsInContactOUs(csentry)) || csentry.ObjectType.Equals(USER) || csentry.ObjectType.Equals(GROUP)) {
				//
				// If the contact is in the authoritative container, then it
				// should project, not join, so this is an error.
				//
				exceptionString = "An authoritative " + csentry.GetType().ToString() + "-object in " + csentry.MA.Name + " with  DN " + csentry.ToString() + " attempts to join when it should be projected.  There " + "is another authoritative object representing the same " + "entity in the metaverse: " + MasterConnector(rgmventry[0]) + ". This object will not be propagated to any other " + "forests until this conflict is resolved by removing " + "or modifying one of the objects so that they no " + "longer collide.  Also Microsoft Identity Integration Server will try to maintain these " + "objects if it has the permissions and may rewrite " + "the attributes unless appropriate action is taken.";

				LogAndThrowUnexpectedDataException(exceptionString);

			}

			//
			// We now have one join candidate for the contact outside the authoritative 
			// containers.
			//
			ConnectedMA MA = rgmventry[0].ConnectedMAs[csentry.MA.Name];
			foreach ( CSEntry entry in MA.Connectors) {
				if (entry.ConnectionRule != RuleType.Provisioning) {
					hasNonProvisionedConnector = entry.DN.ToString();
					break; // TODO: might not be correct. Was : Exit For
				}
			}

			//
			// If there are already non-provisioned connectors from our MA,
			// then it is an error. Otherwise we'll join and disconnect
			// the provisioned connector.
			//

			if (null != hasNonProvisionedConnector) {
				exceptionString = "There are multiple objects representing " + "the same entity in the connectorspace: " + hasNonProvisionedConnector + ", We cannot join, please examine the object " + csentry.ToString() + " and the join rules and determine what attribute(s) " + "caused the conflict.  Please clean up the object to " + "allow it to be propagated to other forests if it is " + "authoritative or use Joiner to connect it to " + "an metaverse object.";

				LogAndThrowUnexpectedDataException(exceptionString);
			}

			//
			// We have found one join candidate and we don't have a non-
			// provisioned connector to it. It is OK to join.
			//
			MVEntry = 0;
			MVObjectClass = rgmventry[0].ObjectType;

			return true;

		}

		public bool ShouldProjectToMV(CSEntry csentry, out string MVObjectClass)
		{

			//
			// Initialize the ObjectClass to nothing
			//
			MVObjectClass = null;

			if (IsUnknownSourceObjectType(csentry)) {
				LogAndThrowUnexpectedDataException("Unhandled object type in ShouldProjectToMV called " + "with entry " + csentry.ToString());
			}

			//
			// Object Type Based Projection
			//
			if (csentry.ObjectType == USER)
			{
				MVObjectClass = ProjectUserToMV(csentry);
			}
			else if (csentry.ObjectType == CONTACT)
			{
				MVObjectClass = ProjectContactToMV(csentry);
			}
			else if (csentry.ObjectType == GROUP || csentry.ObjectType == DYNAMICDDL)
			{
				MVObjectClass = ProjectGroupToMV(csentry);
			}
			else
			{
				LogAndThrowUnexpectedDataException("Unhandled object type in ShouldProjectToMV called " + "with entry " + csentry.ToString());
			}

			//
			// If no value is assigned to MV object class, then we shouldn't 
			// project it else project it
			//
			return MVObjectClass != null;

		}


		public void MapAttributesForImport(string FlowRuleName, CSEntry csentry, MVEntry mventry)
		{
			if (IsUnknownSourceObjectType(csentry)) {
				LogAndThrowUnexpectedDataException("Unhandled object type in MapAttributesForImport called " + "with entry " + csentry.ToString() + " and flow rule name " + FlowRuleName);
			}

			switch (FlowRuleName) {

                case "CalculateMigrated":

                    mventry["migrated"].BooleanValue = false;
                    if (csentry["msExchRecipientTypeDetails"].IsPresent && csentry["msExchRemoteRecipientType"].IsPresent)
                    {
                        if (csentry["msExchRecipientTypeDetails"].IntegerValue > 32)
                        {
                            mventry["migrated"].BooleanValue = true;
                        }
                    }

                    break;


				case "TargetAddressMapping":


                    if (csentry.ObjectType == USER)
                    {
                        // dkegg 06 Jan 2016 - passing MVEntry migrated value along when calling functions
                        bool hasBeenMigrated = hasMailboxBeenMigrated(csentry);

                        IAFTargetAddressForUser(csentry, ref mventry, hasBeenMigrated);
                    }
                    else if (csentry.ObjectType == CONTACT)
                    {
                        IAFTargetAddressForContact(csentry, ref mventry, false);
                    }
                    else if (csentry.ObjectType == GROUP || csentry.ObjectType == DYNAMICDDL)
                    {
                        IAFTargetAddressForGroup(csentry, ref mventry);

                    }
                    else
                    {
                        LogAndThrowUnexpectedDataException("Unhandled object type in MapAttributesForImport " + "called with entry " + csentry.ToString() + " and flow rule name " + FlowRuleName);
							
                    }
					
					break;
				case "LegacyExchangeDNMapping":
					IAFLegacyExchangeDN(ref mventry);

					break;
				case "ExchOriginatingForestMapping":
					IAFExecOriginatingForest(ref csentry, ref mventry);

					break;
				case "MapiRecipientMapping":
					IAFMapiRecipient(csentry, ref mventry);

					break;
				default:
					LogAndThrowUnexpectedDataException("Unhandled flow rule name in MapAttributesForImport " + "called with entry " + csentry.ToString() + " and flow rule name " + FlowRuleName);
					break;
			}
		}


		public void MapAttributesForExport(string FlowRuleName, MVEntry mventry, CSEntry csentry)
		{
			if (IsUnknownObjectType(csentry)) {
				LogAndThrowUnexpectedDataException("Unhandled object type in MapAttributesForExport called " + "with entry " + csentry.ToString() + " and flow rule name " + FlowRuleName);
			}
			switch (FlowRuleName) {

				case "ProxyAddressesMappingForwards":
					EAFProxyAddressesForwards(csentry, ref mventry);

					break;
				case "ProxyAddressesMappingBackwards":
					EAFProxyAddressesBackwards(ref csentry, ref mventry);

					break;
				case "LcsProxyAddressesMappingForwards":
					EAFLcsProxyAddressesForwards(csentry, ref mventry);

					break;
				case "LcsProxyAddressesMappingBackwards":
					EAFLcsProxyAddressesBackwards(ref csentry, ref mventry);

					break;
				case "msExchMasterAccountSidMappingForwards":
					EAFmsExchMasterAccountSidForwards(ref csentry, ref mventry);

					break;
				case "msExchRecipientDisplayTypeMappingForwards":
					EAFmsExchRecipientDisplayTypeForwards(ref csentry, ref mventry);

					break;
				case "msExchMasterAccountHistoryMappingForwards":
					EAFmsExchMasterAccountHistoryForwards(ref csentry, ref mventry);

					break;
				default:
					LogAndThrowUnexpectedDataException("Unhandled flow rule name in MapAttributesForExport " + "called with entry " + csentry.ToString() + " and flow rule name " + FlowRuleName);
					break;
			}

		}

		public DeprovisionAction Deprovision(CSEntry csentry)
		{
			DeprovisionAction functionReturnValue = default(DeprovisionAction);

			if (IsUnknownObjectType(csentry)) {
				LogAndThrowUnexpectedDataException("Unhandled object type in Deprovision called with entry " + csentry.ToString() );
			}

			//
			// if the object is in synchronization ou then we manage it, 
			// so delete it
			//
			if (IsInSynchronizationOU(csentry)) {
				functionReturnValue = DeprovisionAction.Delete;
			} else {
				//
				// so this is an object out of our control, give an error.
				//
				string LogString = "The source object associated with this target object " + csentry.ToString() + " has been deleted, the target object should also be " + "deleted but it is outside the Synchronization OU. " + "Please delete the object manually.";
				//LoggingCs.Log(LogString);
				return DeprovisionAction.Disconnect;
			}
			return functionReturnValue;
		}

		private bool FilterUserObjects(CSEntry csentry)
		{

			if (csentry[HIDE_FROM_ADDRESS_LIST].IsPresent) {
				if (csentry[HIDE_FROM_ADDRESS_LIST].BooleanValue) {
					return true;
				}
			}

			if (!csentry[LEGACY_EXCHANGE_DN].IsPresent) {
				return true;
			}

			if (!csentry[HOME_SERVER_NAME].IsPresent && !csentry[TARGET_ADDRESS].IsPresent) {
				return true;
			}

			if (!csentry[PROXY_ADDRESSES].IsPresent) {
				return true;
			}

			if (csentry[PROXY_ADDRESSES].Values.Count == 0) {
				return true;
			}

			try {
				if (csentry[RECIP_TYPE_DETAILS].IsPresent) {
					long recipientTypeDetails = 0;
					recipientTypeDetails = csentry[RECIP_TYPE_DETAILS].IntegerValue;

					if (recipientTypeDetails == MAILBOX_PLAN || recipientTypeDetails == ARBITRATION_MAILBOX || recipientTypeDetails == DISCOVERY_MAILBOX) {
						return true;
					}
				}
			} catch (NoSuchAttributeException ex) {
				// The msExchRecipientTypeDetails attribute will only be present
				// in the schema for Exchange 2007+ forests.
			}

			return false;
		}

		private bool FilterContactObjects(CSEntry csentry)
		{

			if (Microsoft.MetadirectoryServices.ConnectionState.ExplicitlyConnected == csentry.ConnectionState || Microsoft.MetadirectoryServices.ConnectionState.Connected == csentry.ConnectionState) {
				//
				// Do not disconnect a contact if it is in the sync ou
				//
				if (IsInSynchronizationOU(csentry)) {
					return false;
				}

			}
			//
			// If a contact doesn't have a target address it cannot be exported
			//
			if (!csentry[TARGET_ADDRESS].IsPresent) {
				return true;
			}

			//
			// If hide from address list is present and is true, 
			// this object cannot be exported
			//
			if (csentry[HIDE_FROM_ADDRESS_LIST].IsPresent) {
				if (csentry[HIDE_FROM_ADDRESS_LIST].BooleanValue) {
					return true;
				}
			}

			//
			// If legacy exchange dn attribute is not present, 
			// and this contact is not created by provisioning
			// then this object cannot be exported
			//
			if (!csentry[LEGACY_EXCHANGE_DN].IsPresent) {
				if (!(csentry.ConnectionState == Microsoft.MetadirectoryServices.ConnectionState.Connected && csentry.ConnectionRule == RuleType.Provisioning)) {
					return true;
				}
			}

			//
			// If proxy addresses attribute is not present, this object cannot 
			// be exported
			//
			if (!csentry[PROXY_ADDRESSES].IsPresent) {
				return true;
			}

			//
			// If there are no proxy addresses, this object cannot be exported
			//
			if (csentry[PROXY_ADDRESSES].Values.Count == 0) {
				return true;
			}

			return false;
		}

		private bool FilterGroupObjects(CSEntry csentry)
		{

			//
			// If a group has a primary smtp proxy address, then it can be 
			// exported
			//   otherwise it cannot be exported
			//
			if (PrimarySMTPAddressOfObject(csentry) == null) {
				return true;
			}

			//
			// If hide from address list is present and is true, this object 
			// cannot be exported
			//
			if (csentry[HIDE_FROM_ADDRESS_LIST].IsPresent) {
				if (csentry[HIDE_FROM_ADDRESS_LIST].BooleanValue) {
					return true;
				}
			}

			//
			// If legacy exchange dn attribute is not present, this object 
			// cannot be exported
			//
			if (!csentry[LEGACY_EXCHANGE_DN].IsPresent) {
				return true;
			}

			//
			// If proxy addresses attribute is not present, this object 
			// cannot be exported
			//
			if (!csentry[PROXY_ADDRESSES].IsPresent) {
				return true;
			}

			//
			// If there are no proxy addresses, this object cannot be exported
			//
			if (csentry[PROXY_ADDRESSES].Values.Count == 0) {
				return true;
			}

			return false;
		}

		private string ProjectUserToMV(CSEntry csentry)
		{

			//
			// Unexpected: user in a synchronization ou
			//
			if (IsInSynchronizationOU(csentry))
			{
				LogAndThrowUnexpectedDataException("Authoritative Objects in the Sync OU, " + "please reselect the Sync OU or modify the " + "authoritative objects because they will not be synced: " + csentry.ToString() );
			}

			return PERSON;

		}

		private string ProjectGroupToMV(CSEntry csentry)
		{

			//
			// Unexpected: group in a synchronization ou
			//
			if (IsInSynchronizationOU(csentry))
			{
				LogAndThrowUnexpectedDataException("Authoritative Objects in the Sync OU, " + "please reselect the Sync OU or modify the " + "authoritative objects because they will not be synced: " + csentry.ToString() );
			}

			return GROUP;

		}

		private string ProjectContactToMV(CSEntry csentry)
		{

			//
			// Contacts in a synchronization ou, cannot be projected.
			//
			if (IsInSynchronizationOU(csentry)) {
				return null;
			}

			//
			// Only master contacts can project
			//
			if (!IsInContactOUs(csentry)) {
				return null;
			}

			return GetContactType(csentry);

		}


		private void ExtractFromProxyAddresses(CSEntry csentry, ref ValueCollection values, string searchString)
		{
			//
			// Find Proxy Addresses and put them in join criteria since we are 
			// going to join objects based on Proxy Addresses
			//
			
			searchString = searchString.ToLower();
			foreach ( Value ProxyAddress in csentry[PROXY_ADDRESSES].Values) {

				if (ProxyAddress.ToString().ToLower().StartsWith(searchString)) {
					string[] proxyPair = null;
					proxyPair = ProxyAddress.ToString().Split(PREFIX_SEPARATOR);
					if (null == proxyPair[0] || null == proxyPair[1]) {
						LogAndThrowUnexpectedDataException("Invalid proxy address: " + ProxyAddress.ToString());
					}

					values.Add(proxyPair[1]);

				}
			}

		}


		private void IAFTargetAddressForUser(CSEntry csentry, ref MVEntry mventry, bool hasBeenMigrated)
		{
			//
			// If HomeMDB attribute is not present, the user is a mail 
			// enabled user
			//
			bool IsMailEnabledUser = !csentry[HOME_MDB].IsPresent;

			if (IsMailEnabledUser) {
				//
				// If the user is mail enabled, sync as contact
				//
				IAFTargetAddressForContact(csentry, ref mventry, hasBeenMigrated);
			} else {
				//
				// If the user is mail enabled, sync as group
				//
				IAFTargetAddressForGroup(csentry, ref mventry);
			}

		}


		private void IAFTargetAddressForContact(CSEntry csentry, ref MVEntry mventry, bool hasBeenMigrated)
		{
			//
			// This code should only run if the forest wants emails to the 
			// contacts in this forest to flow through his forest. Otherwise 
			// the mapping is direct not scripted.
			//
			// Find a target address ending with one of the mail domain suffixes
			// if the mail routing is set to TRUE.
			//
			string TargetAddress = null;

            // dkegg 06 Jan 2016 - added hasBeenMigrated boolean to function
            if (hasBeenMigrated)
            {
                TargetAddress = csentry[TARGET_ADDRESS].Value;
            }
            else
            {
                TargetAddress = FindMatchedDomainSuffix(csentry, true, false, hasBeenMigrated);
            }

            //// dkegg 06 Jan 2016 - flip prefix to upper SMTP when using the service routing address as TA
            //if (TargetAddress.ToString().ToLower().Contains("mail.onmicrosoft.com"))
            //{
            //    TargetAddress = "SMTP:" + TargetAddress.ToString().ToLower().Split(':')[1];
            //}

			if (TargetAddress == null) {
				mventry[TARGET_ADDRESS].Value = csentry[TARGET_ADDRESS].Value;
			} else {
				mventry[TARGET_ADDRESS].Value = TargetAddress;
			}

		}


		private void IAFTargetAddressForGroup(CSEntry csentry, ref MVEntry mventry)
		{
			string TargetAddress = null;
            // dkegg 06 Jan 2016 - setting hasBeenMigrated flag to false by default for groups
			TargetAddress = FindMatchedDomainSuffix(csentry, false, false, false);

			if (TargetAddress == null) {
				LogAndThrowUnexpectedDataException("Missing the Unique SMTP proxy from the object " + csentry.ToString() );
			} else {
				mventry[TARGET_ADDRESS].Value = TargetAddress;
			}

		}


		private void EAFProxyAddressesForwards(CSEntry csentry, ref MVEntry mventry)
		{
			//
			// flow to others
			//
		

			if (!csentry.ObjectType.Equals(CONTACT)) {
				LogAndThrowUnexpectedDataException("Unhandled object type in MapProxyAddressesForward " + "called with entry " + csentry.ToString() + " and mventry " + mventry.ToString());
			}

			//
			// We clear all the values first if the object is not in
			// the authoritative container.
			//

            // dkegg - 17 Jan 2016 - configuring GalSync to support the o=ExchangeLabs x500 address added by AAD Connect
            // create a list to hold all x500 addresses with o=ExchangeLabs

            List<string> CloudLegDNs = new List<string>();
            
            if (!IsInContactOUs(csentry)) {
                // loop thru each proxy address, if it contains ExchangeLabs, add to our new list we created
                foreach (var value in csentry[PROXY_ADDRESSES].Values)
                {
                    if (value.ToString().Contains("/o=ExchangeLabs"))
                    {
                        CloudLegDNs.Add(value.ToString());
                    }
                }
                // now we delete the proxy array as per OOTB galsync
                csentry[PROXY_ADDRESSES].Delete();
            }
            // ^ end addition part 1 for Cloud X500 addresses

			foreach (Value value in mventry[PROXY_ADDRESSES].Values)
			{
				string proxyAddress = value.ToString();
				ValueCollection proxyAddressCollection = csentry[PROXY_ADDRESSES].Values;


				InsertProxyAddress(ref proxyAddress, ref proxyAddressCollection);
			}

			//
			// we take all the values from MV and add to cs.
			// NOTE: we can not distinguish whether some MA outside GALSync
			// contributes any values
			//
			foreach ( Value value in mventry[LEGACY_EXCHANGE_DN].Values)
			{

				string x500Address = X500_PREFIX + value.ToString();
				ValueCollection proxyAddressCollection = csentry[PROXY_ADDRESSES].Values;

				InsertProxyAddress(ref x500Address, ref proxyAddressCollection);
			}

            // dkegg - 17 Jan 2016 - configuring GalSync to support the o=ExchangeLabs x500 address added by AAD Connect 
            // add the ExchangeLabs x500 addresses from our new list back to the array

            foreach (var value in CloudLegDNs) 
            {
                string CloudLegDN = value;
                ValueCollection proxyAddressCollection = csentry[PROXY_ADDRESSES].Values;
                InsertProxyAddress(ref CloudLegDN, ref proxyAddressCollection);
            }
            // ^ end addition part 2 for Cloud X500 addresses
		}


		private void EAFLcsProxyAddressesForwards(CSEntry csentry, ref MVEntry mventry)
		{
			if (!csentry.ObjectType.Equals(CONTACT)) {
				LogAndThrowUnexpectedDataException("Unhandled object type in EAFLcsProxyAddressesForwards " + "called with entry " + csentry.ToString() + " and mventry " + mventry.ToString());
			}

			//
			// Process LegacyExchangeDN, proxyAddresses and 
			// metaverse-PrimaryUserAddress (Lcs attribute). Used only when 
			// Lcs is deployed along with Exchange.
			//
			EAFProxyAddressesForwards(csentry, ref mventry);
			if (mventry[SIP_URI].IsPresent)
			{
				string sipUri = mventry[SIP_URI].Value.ToString();
				ValueCollection proxyAddressCollection = csentry[PROXY_ADDRESSES].Values;

                InsertProxyAddress(ref sipUri, ref proxyAddressCollection);
			}

		}



		private void EAFProxyAddressesBackwards(ref CSEntry csentry, ref MVEntry mventry)
		{

			//
			// flow back to itself, do not need to flow MV proxyaddresses
			//

			

			if (IsUnknownObjectType(csentry)) {
				LogAndThrowUnexpectedDataException("Unhandled object type in MapProxyAddressesBackward " + "called with entry " + csentry.ToString() + " and mventry " + mventry.ToString());
			}

			//
			// we take all the values from MV and add to cs.
			// NOTE: we can not distinguish whether some MA outside GALSync
			// contributes any values
			//
			foreach ( Value value in mventry[LEGACY_EXCHANGE_DN].Values)
			{
				string x500Address = X500_PREFIX + value.ToString();
				ValueCollection proxyAddressCollection = csentry[PROXY_ADDRESSES].Values;

				InsertProxyAddress(ref x500Address, ref proxyAddressCollection);
			}

		}



		private void EAFLcsProxyAddressesBackwards(ref CSEntry csentry, ref MVEntry mventry)
		{
			if (IsUnknownObjectType(csentry)) {
				LogAndThrowUnexpectedDataException("Unhandled object type in EAFLcsProxyAddressesBackwards " + "called with entry " + csentry.ToString() + " and mventry " + mventry.ToString());
			}

			//
			// Process both LegacyExchangeDN and 
			// metaverse-PrimaryUserAddress (Lcs attribute). Used only when 
			// Lcs is deployed along with Exchange.
			//
			EAFProxyAddressesBackwards(ref csentry, ref mventry);
			if (mventry[SIP_URI].IsPresent)
			{
				string sipUri = mventry[SIP_URI].Value.ToString();
				ValueCollection proxyAddressCollection = csentry[PROXY_ADDRESSES].Values;

				InsertProxyAddress(ref sipUri, ref proxyAddressCollection);

			;
			}

		}


		private void IAFMapiRecipient(CSEntry csentry, ref MVEntry mventry)
		{
			if (IsUnknownObjectType(csentry)) {
				LogAndThrowUnexpectedDataException("Unhandled object type in EAFMapiRecipient " + "called with entry " + csentry.ToString() + " and mventry " + mventry.ToString());
			}

			if (csentry[HOME_MDB].IsPresent) {
				mventry[MAPI_RECIPIENT].BooleanValue = true;
			} else if ((csentry[MAPI_RECIPIENT].IsPresent && csentry[MAPI_RECIPIENT].BooleanValue == true)) {
				mventry[MAPI_RECIPIENT].BooleanValue = true;
			} else {
				mventry[MAPI_RECIPIENT].BooleanValue = false;
			}

		}


		private void IAFExecOriginatingForest(ref CSEntry csentry, ref MVEntry mventry)
		{
			int index = 0;
			string dotDCName = null;
			string rdn = null;

			for (index = csentry.DN.Depth - 1; index >= 0; index += -1) {
				if (csentry.DN[index].ToUpper().StartsWith("DC=")) {
					rdn = csentry.DN[index].Remove(0, 3);
					if ((null == dotDCName)) {
						dotDCName = rdn;
					} else {
						dotDCName = rdn + "." + dotDCName;
					}
				} else {
					break; // TODO: might not be correct. Was : Exit For
				}
			}

			if (null == dotDCName) {
				LogAndThrowUnexpectedDataException("Unable to find originating forest for " + csentry.ToString() );
			}

			mventry[EXCH_ORIGINATING_FOREST].Value = dotDCName;

		}


		private void InsertProxyAddress(ref string newValue, ref ValueCollection targetValues)
		{
		
			Value demoteValue = null;
			Value deleteValue = null;
			bool find = false;
			string[] targetProxyPair = null;
			string[] newProxyPair = null;

			//
			// Proxy addresses are defined as pairs of "prefix:address".
			// When split by the PREFIX_SEPARATOR, which is ":",
			// the ProxyPair(0) is the prefix, and the ProxyPair(1)
			// is the address.
			//
			newProxyPair = newValue.Split(PREFIX_SEPARATOR);

			if (null == newProxyPair[0] || null == newProxyPair[1]) {
				LogAndThrowUnexpectedDataException("Invalid proxy address: " + newValue);
			}

			foreach ( Value targetValue in targetValues) {
				targetProxyPair = targetValue.ToString().Split(PREFIX_SEPARATOR);

				if (null == targetProxyPair[0] || null == targetProxyPair[1]) {
					LogAndThrowUnexpectedDataException("Invalid proxy address: " + targetValue.ToString());
				}


				if (targetProxyPair[1].ToLower() == newProxyPair[1].ToLower() && (PRIMARY_PROXY != newProxyPair[0] && targetProxyPair[0].ToLower() == newProxyPair[0].ToLower() || PRIMARY_PROXY == newProxyPair[0] && targetProxyPair[0] == newProxyPair[0])) {
					//Semantics for a match:
					//Case 1: the prefix of new address is not  PRIMARY_PROXY(i.e., "SMTP"): case-insensitive match in prefix and case-insensitive match in suffix
					//Case 2: the prefix of the new address  is  PRIMARY_PROXY: exact match in prefix and case-insensitive match in suffix 

					find = true;
					break; // TODO: might not be correct. Was : Exit For
				} else if (PRIMARY_PROXY == newProxyPair[0]) {
					if (PRIMARY_PROXY == targetProxyPair[0]) {
						//
						// If the new primary proxy will be added, we need to
						// demote the existing primary proxy. Assumption here
						// is that there should be at most be one primay 
						// proxy at any time from an value collection.
						//
						demoteValue = targetValue;
					} else if (targetProxyPair[1].ToLower() == newProxyPair[1].ToLower() && PRIMARY_PROXY.ToLower() == targetProxyPair[0]) {
						//
						// If the new primary proxy is promoted from a secondary,
						// the secondary should be deleted.
						//
						deleteValue = targetValue;
					}
				} else if (SIP_PREFIX == newProxyPair[0].ToLower() && SIP_PREFIX == targetProxyPair[0].ToLower()) {
					//
					// If the new addition is "sip:" and a "sip" entry already
					// exists, remove the old one.
					//
					deleteValue = targetValue;
				}
			}

			if ((demoteValue != null)) {
				//
				// Demote the primary proxy by lower casing it
				//
				targetValues.Remove(demoteValue);
				targetProxyPair = demoteValue.ToString().Split(PREFIX_SEPARATOR);
				targetValues.Add(PRIMARY_PROXY.ToLower() + PREFIX_SEPARATOR + targetProxyPair[1]);
			}

			if ((deleteValue != null)) {
				//
				// Delete the secondary proxy which is now promoted to primary
				//
				targetValues.Remove(deleteValue);
			}

			if (false == find) {
				//
				// Add the new one
				//
				targetValues.Add(newValue);
			}

		}


		private void IAFLegacyExchangeDN(ref MVEntry mventry)
		{
			//ConnectedMA ma = default(ConnectedMA);
			//csentry csentry = default(csentry);
			//GALMA MAConfig = default(GALMA);

			//
			// we delete all the values first
			//
			mventry[LEGACY_EXCHANGE_DN].Delete();

			// 
			// add back from all GALSync MAs
			//
			foreach (GALMA MAConfig in galMAs) {
				ConnectedMA ma = mventry.ConnectedMAs[MAConfig.MAName];
				foreach (CSEntry csentry in ma.Connectors) {
					if (csentry[LEGACY_EXCHANGE_DN].IsPresent) {
						mventry[LEGACY_EXCHANGE_DN].Values.Add(csentry[LEGACY_EXCHANGE_DN].Value);
					}
				}
			}
		}

		private string MasterConnector(MVEntry mventry)
		{
			string functionReturnValue = null;

		

			functionReturnValue = "";

			foreach (ConnectedMA ma in mventry.ConnectedMAs) {
				foreach (CSEntry csentry in ma.Connectors) {
					if (csentry.ConnectionRule == RuleType.Projection) {
						functionReturnValue = csentry.ToString();
						return functionReturnValue;
					}
				}
			}

			return "[Master connector not found]";
			
		}


		private void EAFmsExchMasterAccountHistoryForwards(ref CSEntry csentry, ref MVEntry mventry)
		{
			// Variables

			long uac = 0;

			// Clear master account history
			csentry[MASTER_ACCOUNT_HISTORY].Delete();

			// Handle groups
			// Only security mv group object result in stamping of resulting contact when dealing with GROUPS
			GALMA MAConfig = FindMA(csentry);
			bool isTrustEnabled = MAConfig.XFDelegation;

			if (isTrustEnabled && mventry[SID_HISTORY].IsPresent && mventry.ObjectType.Equals(GROUP)) {
				bool isSecurityGroup = (mventry[DISTRIBUTION_GROUP_TYPE].IntegerValue & SECURITY_GROUP_TYPE_CODE) == SECURITY_GROUP_TYPE_CODE;
				if (isSecurityGroup && mventry[MAIL_NICKNAME].IsPresent) {
					csentry[MASTER_ACCOUNT_HISTORY].Value = mventry[SID_HISTORY].Value;
				}
			}

			if (isTrustEnabled && mventry.ObjectType.Equals(PERSON) && mventry[USER_ACCOUNT_CONTROL].IsPresent)
			{
				// Get enabled user, flow SID hositry to Master Account history
				uac = mventry[USER_ACCOUNT_CONTROL].IntegerValue;
				if (((uac & UAC_USER_ACCOUNT) > 0) && !mventry[MASTER_ACCOUNT_SID].IsPresent)
				{

                    // dkegg - 16 Jan 2016 - updated to .values instead of .value
					if (mventry[SID_HISTORY].IsPresent)
					{
						csentry[MASTER_ACCOUNT_HISTORY].Values = mventry[SID_HISTORY].Values;
					}

					// If disabled user and MasterAccountSID is not set to SELFSID, flow MasterAccountSid to master account history
				} else if (((uac & base.UAC_DISABLED_USER) > 0)) {
					if (mventry[MASTER_ACCOUNT_SID].IsPresent && mventry[MASTER_ACCOUNT_SID].Value != SELFSID) {
						csentry[MASTER_ACCOUNT_HISTORY].Value = mventry[MASTER_ACCOUNT_SID].Value;
					}
				}
			}
		}


		private void EAFmsExchMasterAccountSidForwards(ref CSEntry csentry, ref MVEntry mventry)
		{
			// Variables
			long uac = 0;

			// Clear master account history
			csentry[MASTER_ACCOUNT_SID].Delete();


			// Only security mv group object result in stamping of resulting contact when dealing with GROUPS
			GALMA MAConfig = FindMA(csentry);
			bool isTrustEnabled = MAConfig.XFDelegation;
			if ((isTrustEnabled && mventry.ObjectType.Equals(GROUP) && mventry[OBJECT_SID].IsPresent))
			{
				bool isSecurityGroup = (mventry[DISTRIBUTION_GROUP_TYPE].IntegerValue & SECURITY_GROUP_TYPE_CODE) == SECURITY_GROUP_TYPE_CODE;

				if (isSecurityGroup && mventry[MAIL_NICKNAME].IsPresent)
				{
					csentry[MASTER_ACCOUNT_SID].Value = mventry[OBJECT_SID].Value;
				}
			}

			if (isTrustEnabled && mventry.ObjectType.Equals(PERSON) && mventry[USER_ACCOUNT_CONTROL].IsPresent)
			{
				uac = mventry[USER_ACCOUNT_CONTROL].IntegerValue;

				// if enabeld user, flow object SID to master account SID
				if (((uac & UAC_USER_ACCOUNT) > 0) && !mventry[MASTER_ACCOUNT_SID].IsPresent)
				{
					if (mventry[OBJECT_SID].IsPresent)
					{
						csentry[MASTER_ACCOUNT_SID].Value = mventry[OBJECT_SID].Value;
					}

					// if diabled user anbd master account sid not equal self sid, flow master account SID to master account SID
				} 
				else if (((uac & base.UAC_DISABLED_USER) > 0))
				{
					if ((mventry[MASTER_ACCOUNT_SID].IsPresent && mventry[MASTER_ACCOUNT_SID].Value != SELFSID))
					{
						csentry[MASTER_ACCOUNT_SID].Value = mventry[MASTER_ACCOUNT_SID].Value;
					}
				}
			}
		}


		private void EAFmsExchRecipientDisplayTypeForwards(ref CSEntry csentry, ref MVEntry mventry)
		{
			// declare
			int remoteUser = -2147483648;
			int securityPrinciple = 0x40000000;
			int remoteMailUser = 6;
			int localMask = 0xff;
			int remoteMask = 0xff00;
			int rcpDispType = 0;
			GALMA MAConfig = FindMA(csentry);
			bool isTrustEnabled = MAConfig.XFDelegation;
			// See expansion of bits in specs
			int ddlTypeCode = 0x300;
			int distGroupCode = 0x100;
			int secGroupCode = 0x900;

			// Clear display type
			csentry[base.RECIP_DISPLAY_TYPE].Delete();

			// If there is a recipient display type

			if (mventry[base.RECIP_DISPLAY_TYPE].IsPresent) {
				// Only execute for objects that are not groups without nicknames
				if (!(mventry.ObjectType.Equals(GROUP) && !mventry[MAIL_NICKNAME].IsPresent))
				{
					// is the user a security principle?
					rcpDispType = (int)mventry[RECIP_DISPLAY_TYPE].IntegerValue;

					// Create new user tpe
					int userType = (rcpDispType & localMask);
					userType = (userType * 0x100);
					// shift to remote icon
					userType = (userType & remoteMask);
					userType = (userType | remoteUser);

					if (isTrustEnabled) {
						if ((mventry.ObjectType.Equals(GROUP) && mventry[MAIL_NICKNAME].IsPresent && (mventry[DISTRIBUTION_GROUP_TYPE].IsPresent && (mventry[DISTRIBUTION_GROUP_TYPE].IntegerValue & SECURITY_GROUP_TYPE_CODE) == SECURITY_GROUP_TYPE_CODE)) | (mventry.ObjectType.Equals(PERSON) && (((mventry[USER_ACCOUNT_CONTROL].IntegerValue & UAC_DISABLED_USER) > 0 && mventry[MASTER_ACCOUNT_SID].IsPresent && !(mventry[MASTER_ACCOUNT_SID].Value == SELFSID)) | !((mventry[USER_ACCOUNT_CONTROL].IntegerValue & UAC_DISABLED_USER) > 0)))) {
							userType = (userType | securityPrinciple);
						}
					}

					userType = (userType | remoteMailUser);
					csentry[RECIP_DISPLAY_TYPE].IntegerValue = userType;
				}


			} else {
				// No recipent display type, contruct it

				if (mventry.ObjectType.Equals(PERSON) && mventry[USER_ACCOUNT_CONTROL].IsPresent) {
					int recipDisplayTypeBits = 0;

					// Default users to mail users or mail contacts
					recipDisplayTypeBits = remoteUser + remoteMailUser;
					//SyncedMailboxUser (0x80000006)

					// if disabled user
					if (((mventry[USER_ACCOUNT_CONTROL].IntegerValue & UAC_DISABLED_USER) > 0)) {
						//If master account sid <> SELFSID
						if (mventry[MASTER_ACCOUNT_SID].IsPresent && mventry[MASTER_ACCOUNT_SID].Value != SELFSID) {
							// Set remote icon
							recipDisplayTypeBits = recipDisplayTypeBits | (remoteMailUser * 0x100);
							//SyncedRemoteMailUser  (0x80000606)
							if (isTrustEnabled) {
								// Add support for ACL
								recipDisplayTypeBits = (recipDisplayTypeBits | securityPrinciple);
								//ACLableSyncedRemoteMailUser  (0xC0000606)
							}
						}
					} else {
						// An enabled user. 
						// If trust is enabled, add support for ACL on the mailbox.
						if (isTrustEnabled) {
							recipDisplayTypeBits = (recipDisplayTypeBits | securityPrinciple);
							//ACLableSyncedMailboxUser  (0xC0000006) 
						}
					}
					csentry[RECIP_DISPLAY_TYPE].IntegerValue = recipDisplayTypeBits;

				} else if (mventry.ObjectType.Equals(GROUP)) {
					// Check the type of the group
					int recipDisplayTypeBits = 0;
					bool isSecurityGroup = (mventry[DISTRIBUTION_GROUP_TYPE].IntegerValue & SECURITY_GROUP_TYPE_CODE) == SECURITY_GROUP_TYPE_CODE;
					if (mventry[MAIL_NICKNAME].IsPresent) {
						if (isSecurityGroup) {
							recipDisplayTypeBits = remoteUser + remoteMailUser + secGroupCode;
							//SyncedUSGasContact  (0x80000906)
							// security principle only applies to security groups
							if (isTrustEnabled) {
								recipDisplayTypeBits = (recipDisplayTypeBits | securityPrinciple);
								//ACLableSyncedUSGasContact  (0xC0000906)
							}
						} else {
							recipDisplayTypeBits = remoteUser + remoteMailUser + distGroupCode;
							//SyncedUDGasContact  (0x80000106)
						}
						csentry[RECIP_DISPLAY_TYPE].IntegerValue = recipDisplayTypeBits;
					}
				} else if (mventry.ObjectType.Equals(DYNAMICDDL)) {
					csentry[RECIP_DISPLAY_TYPE].IntegerValue = remoteUser + remoteMailUser + ddlTypeCode;
				}

			}
		}
	}

}

