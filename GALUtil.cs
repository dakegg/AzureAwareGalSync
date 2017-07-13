using Microsoft.VisualBasic;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
//=================================================================
//   Copyright (c) Microsoft Corporation.  All rights reserved.
//=================================================================

using Microsoft.Win32;
using Microsoft.MetadirectoryServices;
//Imports Microsoft.MetadirectoryServices.Logging
//using Microsoft.MetadirectoryServices.LoggingCs;
using System.Xml;

namespace MIM.Sync.GALSyncCs
{

	public class GALMA
	{
		public string MAName;
		public string SynchronizationOU;
		public string[] MailDomainNames;
		public string[] ContactOUs;
		public string ContactType;
		public string AdminGroup;
		public bool MailRouting;
		public bool XFDelegation;
	}

	public class Synchronizer
	{

		//
		// Object classes
		//
		protected readonly string USER = "user";
		protected readonly string PERSON = "person";
		protected readonly string CONTACT = "contact";
		protected readonly string GROUP = "group";

		protected readonly string DYNAMICDDL = "msExchDynamicDistributionList";
		//
		// Object attributes
		//
		protected readonly string PROXY_ADDRESSES = "proxyAddresses";
		protected readonly string LEGACY_EXCHANGE_DN = "legacyExchangeDn";
		protected readonly string TARGET_ADDRESS = "targetAddress";
		protected readonly string HOME_MDB = "homeMDB";
		protected readonly string HOME_SERVER_NAME = "msExchHomeServerName";
		protected readonly string MAIL = "mail";
		protected readonly string ORGANIZATION = "o";
		protected readonly string COMMON_NAME = "cn";
		protected readonly string MAPI_RECIPIENT = "MapiRecipient";
		protected readonly string HIDE_FROM_ADDRESS_LIST = "msExchHideFromAddressLists";
		protected readonly string DEPARTMENT = "department";
		protected readonly string EXCH_ORIGINATING_FOREST = "msExchOriginatingForest";
		protected readonly string MAIL_NICK_NAME = "mailNickName";
		protected readonly string MASTER_ACCOUNT_HISTORY = "msExchMasterAccountHistory";
		protected readonly string MASTER_ACCOUNT_SID = "msExchMasterAccountSid";
		protected readonly string OBJECT_SID = "objectSid";
		protected readonly string RECIP_DISPLAY_TYPE = "msExchRecipientDisplayType";
		protected readonly string SELFSID = "AQEAAAAAAAUKAAAA";
		protected readonly string SID_HISTORY = "sIDHistory";
		protected readonly string USER_ACCOUNT_CONTROL = "userAccountControl";
		protected readonly string MAIL_NICKNAME = "mailNickname";
		protected readonly long UAC_DISABLED_USER = 2;
		protected readonly long UAC_USER_ACCOUNT = 0x200;
		protected readonly string DISTRIBUTION_GROUP_TYPE = "groupType";
		protected readonly int SECURITY_GROUP_TYPE_CODE = -2147483648;

		protected readonly string RECIP_TYPE_DETAILS = "msExchRecipientTypeDetails";
		//
		// msExchRecipientTypeDetails constants
		//
		protected readonly long MAILBOX_PLAN = 0x1000000;
		protected readonly long ARBITRATION_MAILBOX = 0x800000;

		protected readonly long DISCOVERY_MAILBOX = 0x20000000;
		//
		// Proxy prefixes
		//
		protected readonly char PREFIX_SEPARATOR = ':';
		protected readonly string X500_PREFIX = "X500:";
		protected readonly string SMTP_PREFIX = "SMTP:";

		protected readonly string PRIMARY_PROXY = "SMTP";
		// 
		// LCS constants
		//
		protected readonly string SIP_URI = "msRTCSIP-PrimaryUserAddress";

		protected readonly string SIP_PREFIX = "sip";
		//
		// XML configuration tags
		//
		const string MMS_RULES_EXTENSION = "rules-extension-properties";
		const string GAL_SYNC_MAS = "galsync-mas";
		const string GAL_SYNC_MA_CONTACT_TYPE = "contact-type";
		const string GAL_SYNC_MA_ADMIN_GROUP = "admin-group";
		const string GAL_SYNC_MA_TARGET_OU = "target-ou";
		const string GAL_SYNC_MA_SMTP_MAIL_DOMAINS = "smtp-mail-domains";
		const string GAL_SYNC_MA_SOURCE_CONTACT_OUS = "source-contact-ous";
		const string GAL_SYNC_MA_MAIL_ROUTING = "mail-routing";
		const string GAL_SYNC_MA = "galma";
		const string GAL_SYNC_MA_DOMAIN = "domain";
		const string GAL_SYNC_MA_OU = "ou";

		const string GAL_SYNC_MA_XF_DELEGATION = "cross-forest-delegation";
		//
		// Configuration information
		//

		protected GALMA[] galMAs;
		// 
		// Definition of allowed characters in LegacyExchangeDN. 
		// This definition is according to AD common name mapping.
		//
		// Column 0 is what the character should be mapped to.
		// Column 1 is what the charecter is mapped from, which is defined
		//   only for commenting purpose since VB won't allow comments 
		//   with line continuations.
		// All 0-255 charaters are defined in the exact sequence.
		//
		private char[,] LegacyDNAnsiMap = {
			{
				'?',
				Strings.ChrW(0x0)
			},
			{
				'?',
				Strings.ChrW(0x1)
			},
			{
				'?',
				Strings.ChrW(0x2)
			},
			{
				'?',
				Strings.ChrW(0x3)
			},
			{
				'?',
				Strings.ChrW(0x4)
			},
			{
				'?',
				Strings.ChrW(0x5)
			},
			{
				'?',
				Strings.ChrW(0x6)
			},
			{
				'?',
				Strings.ChrW(0x7)
			},
			{
				'?',
				Strings.ChrW(0x8)
			},
			{
				'?',
				Strings.ChrW(0x9)
			},
			{
				'?',
				Strings.ChrW(0xa)
			},
			{
				'?',
				Strings.ChrW(0xb)
			},
			{
				'?',
				Strings.ChrW(0xc)
			},
			{
				'?',
				Strings.ChrW(0xd)
			},
			{
				'?',
				Strings.ChrW(0xe)
			},
			{
				'?',
				Strings.ChrW(0xf)
			},
			{
				'?',
				Strings.ChrW(0x0)
			},
			{
				'?',
				Strings.ChrW(0x11)
			},
			{
				'?',
				Strings.ChrW(0x12)
			},
			{
				'?',
				Strings.ChrW(0x13)
			},
			{
				'?',
				Strings.ChrW(0x14)
			},
			{
				'?',
				Strings.ChrW(0x15)
			},
			{
				'?',
				Strings.ChrW(0x16)
			},
			{
				'?',
				Strings.ChrW(0x17)
			},
			{
				'?',
				Strings.ChrW(0x18)
			},
			{
				'?',
				Strings.ChrW(0x19)
			},
			{
				'?',
				Strings.ChrW(0x1a)
			},
			{
				'?',
				Strings.ChrW(0x1b)
			},
			{
				'?',
				Strings.ChrW(0x1c)
			},
			{
				'?',
				Strings.ChrW(0x1d)
			},
			{
				'?',
				Strings.ChrW(0x1e)
			},
			{
				'?',
				Strings.ChrW(0x1f)
			},
			{
				' ',
				Strings.ChrW(0x20)
			},
			{
				'!',
				Strings.ChrW(0x21)
			},
			{
				'\'',
				Strings.ChrW(0x22)
			},
			{
				'?',
				Strings.ChrW(0x23)
			},
			{
				'?',
				Strings.ChrW(0x24)
			},
			{
				'%',
				Strings.ChrW(0x25)
			},
			{
				'&',
				Strings.ChrW(0x26)
			},
			{
				'\'',
				Strings.ChrW(0x27)
			},
			{
				'(',
				Strings.ChrW(0x28)
			},
			{
				')',
				Strings.ChrW(0x29)
			},
			{
				'*',
				Strings.ChrW(0x2a)
			},
			{
				'+',
				Strings.ChrW(0x2b)
			},
			{
				',',
				Strings.ChrW(0x2c)
			},
			{
				'-',
				Strings.ChrW(0x2d)
			},
			{
				'.',
				Strings.ChrW(0x2e)
			},
			{
				'?',
				Strings.ChrW(0x2f)
			},
			{
				'0',
				Strings.ChrW(0x30)
			},
			{
				'1',
				Strings.ChrW(0x31)
			},
			{
				'2',
				Strings.ChrW(0x32)
			},
			{
				'3',
				Strings.ChrW(0x33)
			},
			{
				'4',
				Strings.ChrW(0x34)
			},
			{
				'5',
				Strings.ChrW(0x35)
			},
			{
				'6',
				Strings.ChrW(0x36)
			},
			{
				'7',
				Strings.ChrW(0x37)
			},
			{
				'8',
				Strings.ChrW(0x38)
			},
			{
				'9',
				Strings.ChrW(0x39)
			},
			{
				':',
				Strings.ChrW(0x3a)
			},
			{
				'?',
				Strings.ChrW(0x3b)
			},
			{
				'<',
				Strings.ChrW(0x3c)
			},
			{
				'=',
				Strings.ChrW(0x3d)
			},
			{
				'>',
				Strings.ChrW(0x3e)
			},
			{
				'?',
				Strings.ChrW(0x3f)
			},
			{
				'@',
				Strings.ChrW(0x40)
			},
			{
				'A',
				Strings.ChrW(0x41)
			},
			{
				'B',
				Strings.ChrW(0x42)
			},
			{
				'C',
				Strings.ChrW(0x43)
			},
			{
				'D',
				Strings.ChrW(0x44)
			},
			{
				'E',
				Strings.ChrW(0x45)
			},
			{
				'F',
				Strings.ChrW(0x46)
			},
			{
				'G',
				Strings.ChrW(0x47)
			},
			{
				'H',
				Strings.ChrW(0x48)
			},
			{
				'I',
				Strings.ChrW(0x49)
			},
			{
				'J',
				Strings.ChrW(0x4a)
			},
			{
				'K',
				Strings.ChrW(0x4b)
			},
			{
				'L',
				Strings.ChrW(0x4c)
			},
			{
				'M',
				Strings.ChrW(0x4d)
			},
			{
				'N',
				Strings.ChrW(0x4e)
			},
			{
				'O',
				Strings.ChrW(0x4f)
			},
			{
				'P',
				Strings.ChrW(0x50)
			},
			{
				'Q',
				Strings.ChrW(0x51)
			},
			{
				'R',
				Strings.ChrW(0x52)
			},
			{
				'S',
				Strings.ChrW(0x53)
			},
			{
				'T',
				Strings.ChrW(0x54)
			},
			{
				'U',
				Strings.ChrW(0x55)
			},
			{
				'V',
				Strings.ChrW(0x56)
			},
			{
				'W',
				Strings.ChrW(0x57)
			},
			{
				'X',
				Strings.ChrW(0x58)
			},
			{
				'Y',
				Strings.ChrW(0x59)
			},
			{
				'Z',
				Strings.ChrW(0x5a)
			},
			{
				'[',
				Strings.ChrW(0x5b)
			},
			{
				'?',
				Strings.ChrW(0x5c)
			},
			{
				']',
				Strings.ChrW(0x5d)
			},
			{
				'?',
				Strings.ChrW(0x5e)
			},
			{
				'_',
				Strings.ChrW(0x5f)
			},
			{
				'?',
				Strings.ChrW(0x60)
			},
			{
				'a',
				Strings.ChrW(0x61)
			},
			{
				'b',
				Strings.ChrW(0x62)
			},
			{
				'c',
				Strings.ChrW(0x63)
			},
			{
				'd',
				Strings.ChrW(0x64)
			},
			{
				'e',
				Strings.ChrW(0x65)
			},
			{
				'f',
				Strings.ChrW(0x66)
			},
			{
				'g',
				Strings.ChrW(0x67)
			},
			{
				'h',
				Strings.ChrW(0x68)
			},
			{
				'i',
				Strings.ChrW(0x69)
			},
			{
				'j',
				Strings.ChrW(0x6a)
			},
			{
				'k',
				Strings.ChrW(0x6b)
			},
			{
				'l',
				Strings.ChrW(0x6c)
			},
			{
				'm',
				Strings.ChrW(0x6d)
			},
			{
				'n',
				Strings.ChrW(0x6e)
			},
			{
				'o',
				Strings.ChrW(0x6f)
			},
			{
				'p',
				Strings.ChrW(0x70)
			},
			{
				'q',
				Strings.ChrW(0x71)
			},
			{
				'r',
				Strings.ChrW(0x72)
			},
			{
				's',
				Strings.ChrW(0x73)
			},
			{
				't',
				Strings.ChrW(0x74)
			},
			{
				'u',
				Strings.ChrW(0x75)
			},
			{
				'v',
				Strings.ChrW(0x76)
			},
			{
				'w',
				Strings.ChrW(0x77)
			},
			{
				'x',
				Strings.ChrW(0x78)
			},
			{
				'y',
				Strings.ChrW(0x79)
			},
			{
				'z',
				Strings.ChrW(0x7a)
			},
			{
				'?',
				Strings.ChrW(0x7b)
			},
			{
				'|',
				Strings.ChrW(0x7c)
			},
			{
				'?',
				Strings.ChrW(0x7d)
			},
			{
				'?',
				Strings.ChrW(0x7e)
			},
			{
				'?',
				Strings.ChrW(0x7f)
			},
			{
				'?',
				Strings.ChrW(0x80)
			},
			{
				'?',
				Strings.ChrW(0x81)
			},
			{
				'?',
				Strings.ChrW(0x82)
			},
			{
				'?',
				Strings.ChrW(0x83)
			},
			{
				'?',
				Strings.ChrW(0x84)
			},
			{
				'?',
				Strings.ChrW(0x85)
			},
			{
				'?',
				Strings.ChrW(0x86)
			},
			{
				'?',
				Strings.ChrW(0x87)
			},
			{
				'?',
				Strings.ChrW(0x88)
			},
			{
				'?',
				Strings.ChrW(0x89)
			},
			{
				'S',
				Strings.ChrW(0x8a)
			},
			{
				'?',
				Strings.ChrW(0x8b)
			},
			{
				'?',
				Strings.ChrW(0x8c)
			},
			{
				'?',
				Strings.ChrW(0x8d)
			},
			{
				'?',
				Strings.ChrW(0x8e)
			},
			{
				'?',
				Strings.ChrW(0x8f)
			},
			{
				'?',
				Strings.ChrW(0x90)
			},
			{
				'?',
				Strings.ChrW(0x91)
			},
			{
				'?',
				Strings.ChrW(0x92)
			},
			{
				'?',
				Strings.ChrW(0x93)
			},
			{
				'?',
				Strings.ChrW(0x94)
			},
			{
				'?',
				Strings.ChrW(0x95)
			},
			{
				'?',
				Strings.ChrW(0x96)
			},
			{
				'?',
				Strings.ChrW(0x97)
			},
			{
				'?',
				Strings.ChrW(0x98)
			},
			{
				'?',
				Strings.ChrW(0x99)
			},
			{
				's',
				Strings.ChrW(0x9a)
			},
			{
				'?',
				Strings.ChrW(0x9b)
			},
			{
				'?',
				Strings.ChrW(0x9c)
			},
			{
				'?',
				Strings.ChrW(0x9d)
			},
			{
				'?',
				Strings.ChrW(0x9e)
			},
			{
				'Y',
				Strings.ChrW(0x9f)
			},
			{
				'?',
				Strings.ChrW(0xa0)
			},
			{
				'?',
				Strings.ChrW(0xa1)
			},
			{
				'C',
				Strings.ChrW(0xa2)
			},
			{
				'L',
				Strings.ChrW(0xa3)
			},
			{
				'P',
				Strings.ChrW(0xa4)
			},
			{
				'Y',
				Strings.ChrW(0xa5)
			},
			{
				'I',
				Strings.ChrW(0xa6)
			},
			{
				'S',
				Strings.ChrW(0xa7)
			},
			{
				'?',
				Strings.ChrW(0xa8)
			},
			{
				'C',
				Strings.ChrW(0xa9)
			},
			{
				'A',
				Strings.ChrW(0xaa)
			},
			{
				'?',
				Strings.ChrW(0xab)
			},
			{
				'?',
				Strings.ChrW(0xac)
			},
			{
				'?',
				Strings.ChrW(0xad)
			},
			{
				'R',
				Strings.ChrW(0xae)
			},
			{
				'?',
				Strings.ChrW(0xaf)
			},
			{
				'?',
				Strings.ChrW(0xb0)
			},
			{
				'?',
				Strings.ChrW(0xb1)
			},
			{
				'2',
				Strings.ChrW(0xb2)
			},
			{
				'3',
				Strings.ChrW(0xb3)
			},
			{
				'?',
				Strings.ChrW(0xb4)
			},
			{
				'M',
				Strings.ChrW(0xb5)
			},
			{
				'P',
				Strings.ChrW(0xb6)
			},
			{
				'?',
				Strings.ChrW(0xb7)
			},
			{
				'?',
				Strings.ChrW(0xb8)
			},
			{
				'1',
				Strings.ChrW(0xb9)
			},
			{
				'O',
				Strings.ChrW(0xba)
			},
			{
				'?',
				Strings.ChrW(0xbb)
			},
			{
				'?',
				Strings.ChrW(0xbc)
			},
			{
				'?',
				Strings.ChrW(0xbd)
			},
			{
				'?',
				Strings.ChrW(0xbe)
			},
			{
				'?',
				Strings.ChrW(0xbf)
			},
			{
				'A',
				Strings.ChrW(0xc0)
			},
			{
				'A',
				Strings.ChrW(0xc1)
			},
			{
				'A',
				Strings.ChrW(0xc2)
			},
			{
				'A',
				Strings.ChrW(0xc3)
			},
			{
				'A',
				Strings.ChrW(0xc4)
			},
			{
				'A',
				Strings.ChrW(0xc5)
			},
			{
				'A',
				Strings.ChrW(0xc6)
			},
			{
				'C',
				Strings.ChrW(0xc7)
			},
			{
				'E',
				Strings.ChrW(0xc8)
			},
			{
				'E',
				Strings.ChrW(0xc9)
			},
			{
				'E',
				Strings.ChrW(0xca)
			},
			{
				'E',
				Strings.ChrW(0xcb)
			},
			{
				'I',
				Strings.ChrW(0xcc)
			},
			{
				'I',
				Strings.ChrW(0xcd)
			},
			{
				'I',
				Strings.ChrW(0xce)
			},
			{
				'I',
				Strings.ChrW(0xcf)
			},
			{
				'D',
				Strings.ChrW(0xd0)
			},
			{
				'N',
				Strings.ChrW(0xd1)
			},
			{
				'O',
				Strings.ChrW(0xd2)
			},
			{
				'O',
				Strings.ChrW(0xd3)
			},
			{
				'O',
				Strings.ChrW(0xd4)
			},
			{
				'O',
				Strings.ChrW(0xd5)
			},
			{
				'O',
				Strings.ChrW(0xd6)
			},
			{
				'X',
				Strings.ChrW(0xd7)
			},
			{
				'0',
				Strings.ChrW(0xd8)
			},
			{
				'U',
				Strings.ChrW(0xd9)
			},
			{
				'U',
				Strings.ChrW(0xda)
			},
			{
				'U',
				Strings.ChrW(0xdb)
			},
			{
				'U',
				Strings.ChrW(0xdc)
			},
			{
				'Y',
				Strings.ChrW(0xdd)
			},
			{
				'T',
				Strings.ChrW(0xde)
			},
			{
				'?',
				Strings.ChrW(0xdf)
			},
			{
				'a',
				Strings.ChrW(0xe0)
			},
			{
				'a',
				Strings.ChrW(0xe1)
			},
			{
				'a',
				Strings.ChrW(0xe2)
			},
			{
				'a',
				Strings.ChrW(0xe3)
			},
			{
				'a',
				Strings.ChrW(0xe4)
			},
			{
				'a',
				Strings.ChrW(0xe5)
			},
			{
				'?',
				Strings.ChrW(0xe6)
			},
			{
				'c',
				Strings.ChrW(0xe7)
			},
			{
				'e',
				Strings.ChrW(0xe8)
			},
			{
				'e',
				Strings.ChrW(0xe9)
			},
			{
				'e',
				Strings.ChrW(0xea)
			},
			{
				'e',
				Strings.ChrW(0xeb)
			},
			{
				'i',
				Strings.ChrW(0xec)
			},
			{
				'i',
				Strings.ChrW(0xed)
			},
			{
				'i',
				Strings.ChrW(0xee)
			},
			{
				'i',
				Strings.ChrW(0xef)
			},
			{
				'd',
				Strings.ChrW(0xf0)
			},
			{
				'n',
				Strings.ChrW(0xf1)
			},
			{
				'o',
				Strings.ChrW(0xf2)
			},
			{
				'o',
				Strings.ChrW(0xf3)
			},
			{
				'o',
				Strings.ChrW(0xf4)
			},
			{
				'o',
				Strings.ChrW(0xf5)
			},
			{
				'o',
				Strings.ChrW(0xf6)
			},
			{
				'?',
				Strings.ChrW(0xf7)
			},
			{
				'o',
				Strings.ChrW(0xf8)
			},
			{
				'u',
				Strings.ChrW(0xf9)
			},
			{
				'u',
				Strings.ChrW(0xfa)
			},
			{
				'u',
				Strings.ChrW(0xfb)
			},
			{
				'u',
				Strings.ChrW(0xfc)
			},
			{
				'y',
				Strings.ChrW(0xfd)
			},
			{
				'T',
				Strings.ChrW(0xfe)
			},
			{
				'y',
				Strings.ChrW(0xff)
			}

		};

		protected GALMA FindMA(CSEntry csentry)
		{

			int i = 0;

			for (i = 0; i <= galMAs.Length - 1; i++) {
				if (galMAs[i].MAName.ToLower() == csentry.MA.Name.ToLower()) {
					return galMAs[i];
				}
			}

			throw new UnexpectedDataException("MA cannot be found: " + csentry.MA.Name);

		}

		protected bool IsInSynchronizationOU(CSEntry csentry)
		{
			//
			// Find out if the object is in one of the synchronization OUs, 
			// meaning we are managing this object
			//
			GALMA MAConfig = null;
			MAConfig = FindMA(csentry);
			//
			// To be in a synchronization OU, the parent ou of the object 
			// must be in the list of synchronization OUs
			//
			if ((MAConfig.SynchronizationOU == null) || MAConfig.SynchronizationOU.Equals("")) {
				return false;
			} else {
				return csentry.DN.ToString().ToLower().EndsWith(MAConfig.SynchronizationOU.ToString().ToLower());
			}
		}

		protected bool IsInContactOUs(CSEntry csentry)
		{

			//
			// Find out if the object is in one of the Contact OUs, 
			// meaning it is a canditate for projection
			//
			GALMA MAConfig = null;
			string ContactOU = null;

			//
			// To be in a Contact OU, the parent ou of the object must 
			// be in the list of Contact OUs
			//
			MAConfig = FindMA(csentry);

			//
			// if contact OU is not defined
			//
			if ((MAConfig.ContactOUs == null)) {
				return false;
			}

			foreach (string ContactOU_loopVariable in MAConfig.ContactOUs) {
				ContactOU = ContactOU_loopVariable;
				if (csentry.DN.ToString().ToLower().EndsWith(ContactOU.ToString().ToLower())) {
					return true;
				}
			}
			return false;
		}

		protected string GetContactType(CSEntry csentry)
		{

			//
			// Find out if the object is in one of the Contact OUs, 
			// meaning it is a canditate for projection
			//
			GALMA MAConfig = null;

			//
			// To be in a Contact OU, the parent ou of the object must 
			// be in the list of Contact OUs
			//
			MAConfig = FindMA(csentry);

			//
			// if contact OU is not defined
			//
			if ((MAConfig.ContactType == null) || MAConfig.ContactType.Equals("")) {
				return null;
			}

			return MAConfig.ContactType;

		}

		protected string GetAdminGroup(CSEntry csentry)
		{

			//
			// Find out if the object is in one of the Contact OUs, 
			// meaning it is a canditate for projection
			//
			GALMA MAConfig = null;

			//
			// To be in a Contact OU, the parent ou of the object must 
			// be in the list of Contact OUs
			//
			MAConfig = FindMA(csentry);

			//
			// if contact OU is not defined
			//
			if ((MAConfig.AdminGroup == null) || MAConfig.AdminGroup.Equals("")) {
				return null;
			}

			return MAConfig.AdminGroup;

		}

		protected void GetConfigurationData()
		{
			//
			// do not catch exception
			//
			XmlDocument doc = new XmlDocument();
			int i = 0;
			int j = 0;
			XmlNodeList nodeMAList = default(XmlNodeList);
			XmlNode nodeMA = default(XmlNode);
			XmlNode node = default(XmlNode);
	
			XmlNode nodeMAs = default(XmlNode);
			GALMA MAConfig = null;

			doc.Load(Utils.ExtensionsDirectory + "\\GALSync.xml");

			nodeMAs = doc.SelectSingleNode("/" + MMS_RULES_EXTENSION + "/" + GAL_SYNC_MAS);
			nodeMAList = nodeMAs.SelectNodes(GAL_SYNC_MA);

			galMAs = new GALMA[nodeMAList.Count];

			for (i = 0; i <= nodeMAList.Count - 1; i++) {
				nodeMA = nodeMAList.Item(i);

				galMAs[i] = new GALMA();
				MAConfig = galMAs[i];
				MAConfig.MAName = null;
				MAConfig.SynchronizationOU = null;
				MAConfig.MailDomainNames = null;
				MAConfig.ContactOUs = null;
				MAConfig.ContactType = null;
				MAConfig.AdminGroup = null;
				MAConfig.MailRouting = false;
				MAConfig.XFDelegation = false;

				// Find MA Name
				foreach (XmlAttribute maAttribute in nodeMA.Attributes) {
					if ("name" == maAttribute.Name) {
						MAConfig.MAName = maAttribute.InnerText.Trim();
						break; // TODO: might not be correct. Was : Exit For
					}
				}

				if ((MAConfig.MAName == null)) {
					throw new UnexpectedDataException("No MA name Attribute in configuration XML");
				}

				// Read contact type
				node = nodeMA.SelectSingleNode(GAL_SYNC_MA_CONTACT_TYPE);
				if ((node != null)) {
					MAConfig.ContactType = node.InnerText.Trim();
				}

				// Read admin group
				node = nodeMA.SelectSingleNode(GAL_SYNC_MA_ADMIN_GROUP);
				if ((node != null)) {
					MAConfig.AdminGroup = node.InnerText.Trim();
				}

				// Read target OU
				node = nodeMA.SelectSingleNode(GAL_SYNC_MA_TARGET_OU);
				if ((node != null)) {
					MAConfig.SynchronizationOU = node.InnerText.Trim();
				}

				// Read mail routing
				node = nodeMA.SelectSingleNode(GAL_SYNC_MA_MAIL_ROUTING);
				if ((node != null) && node.InnerText.Trim().ToLower() == "true") {
					MAConfig.MailRouting = true;
				} else {
					MAConfig.MailRouting = false;
				}

				// Read mail domains
				node = nodeMA.SelectSingleNode(GAL_SYNC_MA_SMTP_MAIL_DOMAINS);

				if ((node != null) && 0 < node.ChildNodes.Count) {
					
					MAConfig.MailDomainNames = (string[])Array.CreateInstance(typeof(string), node.ChildNodes.Count);

					for (j = 0; j <= node.ChildNodes.Count - 1; j++) {
						if (GAL_SYNC_MA_DOMAIN == node.ChildNodes[j].Name) {
							MAConfig.MailDomainNames[j] = node.ChildNodes[j].InnerText.Trim();
						} else {
							// Since we allocated the array accordingly
							throw new UnexpectedDataException("Unexpected mail domain");
						}
					}
				}

				// Read contact OUs
				node = nodeMA.SelectSingleNode(GAL_SYNC_MA_SOURCE_CONTACT_OUS);

				if ((node != null) && 0 < node.ChildNodes.Count) {
					MAConfig.ContactOUs = (string[])Array.CreateInstance(typeof(string), node.ChildNodes.Count);

					for (j = 0; j <= node.ChildNodes.Count - 1; j++) {
						if (GAL_SYNC_MA_OU == node.ChildNodes[j].Name) {
							MAConfig.ContactOUs[j] = node.ChildNodes[j].InnerText.Trim();
						} else {
							// Since we allocated the array accordingly
							throw new UnexpectedDataException("Unexpected contact ou");
						}
					}
				}
				// Read cross forest delgation
				node = nodeMA.SelectSingleNode(GAL_SYNC_MA_XF_DELEGATION);
				if ((node != null) && node.InnerText.Trim().ToLower() == "true") {
					MAConfig.XFDelegation = true;
				} else {
					MAConfig.XFDelegation = false;
				}
			}

		}

        // added bool hasBeenMigrated to use in suffix selection
		protected string FindMatchedDomainSuffix(CSEntry csentry, bool checkMailRouting, bool onlyMatchingFirstSuffix, bool hasBeenMigrated)
		{

			string result = null;

			GALMA MAConfig = null;

			//
			// CheckMailRouting is true if called for Contact,
			// false for user and group.
			//
			// So for contact (true == checkMailRouting), if the MA is 
			// configured to use mail routing, then skip finding from
			// matching proxy, the caller will then map the source CS
			// target address to MV directly.
			// 
			if (true == checkMailRouting)
			{
				MAConfig = FindMA(csentry);
				if (false == MAConfig.MailRouting)
				{
					result = null;
				}
			}

			//
			// Check every smtp address in proxy addresses if they end 
			// with one of the mail domain suffixes that that forest 
			// controls return it
			//
			foreach (Value ProxyAddress in csentry[PROXY_ADDRESSES].Values)
			{
				string ProxyAddressString = ProxyAddress.ToString();

                // dkegg 06 Jan 2016 - added hasBeenMigrated to logic for smtp selection in function call
				if (ProxyAddressString.ToUpper().StartsWith(SMTP_PREFIX))
				{
					if (ProxyAddressIsInSMTPMailDomain(csentry, ProxyAddressString, onlyMatchingFirstSuffix, hasBeenMigrated))
					{
						result = ProxyAddressString;
					}
				}
			}

			return result;
		}

		

	

		private bool ProxyAddressIsInSMTPMailDomain(CSEntry csentry, string ProxyAddress, bool onlyMatchingFirstSuffix, bool hasBeenMigrated)
		{

			GALMA MAConfig = null;
			string MailDomainSuffix = null;

			//
			// Find the index of the MA that csentry is in
			//
			MAConfig = FindMA(csentry);

			//
			// if no domain names defined
			//

			if ((MAConfig.MailDomainNames == null) || (MAConfig.MailDomainNames == null)) {
				throw new TerminateRunException("Mail suffixes are not defined for MA: " + MAConfig.MAName);
			}

			//
			// Check if the given proxy address ends with one the 
			// mail domain suffixes for that MA
			//
			foreach (string MailDomainSuffix_loopVariable in MAConfig.MailDomainNames) {
				MailDomainSuffix = MailDomainSuffix_loopVariable;

                // dkegg 06 Jan 2016 - checking for mail.onmicrosoft.com address first and returning it 
                if (hasBeenMigrated && ProxyAddress.ToLower().Contains("mail.onmicrosoft.com"))
                {
                    return true;
                }

				if (ProxyAddress.ToLower().EndsWith(MailDomainSuffix.ToString().ToLower())) {
					return true;
				}

				if (onlyMatchingFirstSuffix) {
					return false;
				}
			}

			return false;
		}
       

        // dkegg 06 Jan 2016 - added to calculate if migration has occurred

        protected bool hasMailboxBeenMigrated(CSEntry csentry)
        {
            bool result = false;

            if (csentry["msExchRecipientTypeDetails"].IsPresent && csentry["msExchRemoteRecipientType"].IsPresent)
            {
                if (csentry["msExchRecipientTypeDetails"].IntegerValue > 32)
                {
                    result = true;
                }
            }
            return result;
        }

		protected bool IsUnknownObjectType(CSEntry csentry)
		{

			return IsUnknownSourceObjectType(csentry) && IsUnknownTargetObjectType(csentry);

		}

		protected bool IsUnknownSourceObjectType(CSEntry csentry)
		{
			bool result = true;
			


			if (csentry.ObjectType == USER || csentry.ObjectType == CONTACT || csentry.ObjectType == GROUP)
			{
				result = false;
			}
			else if (csentry.ObjectType == DYNAMICDDL)
			{
				result = false;
			}
			else
			{
				result = true;
			}

			return result;
		}

		protected bool IsUnknownTargetObjectType(CSEntry csentry)
		{
			bool result = true;
			if (csentry.ObjectType == CONTACT)
			{
				result = false;
			}
			else
			{
				result = true;
			}


			return result;
		}

		protected string PrimarySMTPAddressOfObject(CSEntry csentry)
		{

			
			foreach ( Value ProxyAddress in csentry[PROXY_ADDRESSES].Values) {
				if (ProxyAddress.ToString().StartsWith("SMTP:")) {
					return ProxyAddress.ToString();
				}
			}
			return null;

		}


		protected void LogAndThrowUnexpectedDataException(string ExceptionString)
		{
			//LoggingCs.Log("Error - " + ExceptionString);
			throw new UnexpectedDataException(ExceptionString);
		}

		protected string ValidateLegacyExhangeDN(char[] LegacyExchangeDN)
		{

			int index = 0;
			int cValue = 0;
			string ValidatedDN = "";


			for (index = 0; index <= LegacyExchangeDN.Length - 1; index++) {
				cValue = Strings.AscW(LegacyExchangeDN[index]);

				if (cValue <= 255 && cValue >= 0) {
					if ('?' != LegacyDNAnsiMap[cValue, 0]) {
						ValidatedDN = ValidatedDN + LegacyDNAnsiMap[cValue, 0];
					}

				}
			}

			if (0 != ValidatedDN.Length) {
				return ValidatedDN;
			} else {
				return null;
			}

		}

	}

}

