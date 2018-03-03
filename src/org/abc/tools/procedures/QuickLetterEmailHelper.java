package org.abc.tools.procedures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import com.follett.fsc.core.k12.beans.Organization;
import com.follett.fsc.core.k12.beans.Person;
import com.follett.fsc.core.k12.beans.Student;
import com.follett.fsc.core.k12.beans.StudentContact;
import com.follett.fsc.core.k12.beans.User;
import com.follett.fsc.core.k12.beans.X2BaseBean;
import com.follett.fsc.core.k12.business.MessageProperties;
import com.follett.fsc.core.k12.business.WriteEmailManager;
import com.follett.fsc.core.k12.web.UserDataContainer;

/**
 * This helps QuickLetterData subclasses send emails. (By default a QuickLetter
 * only produces pages of letters for the user to print, but this can also email
 * those letters.)
 * <p>
 * The recommended way to hook this up to your QuickLetterData subclass is as
 * follows:
 * 
 * <pre>
 * 
 * QuickLetterEmailHelper emailHelper;
 * 
 * &#064;Override
 * protected String resolveLetterBodyCalculation(String startingHtml,
 * 		X2BaseBean bean) {
 * 	String updatedHtml = super.resolveLetterBodyCalculation(startingHtml, bean);
 * 	emailHelper.updateSection(bean, startingHtml, updatedHtml);
 * 	return updatedHtml;
 * }
 * 
 * &#064;Override
 * protected String resolveLetterBodyExpression(String startingHtml,
 * 		X2BaseBean bean) {
 * 	String updatedHtml = super.resolveLetterBodyExpression(startingHtml, bean);
 * 	emailHelper.updateSection(bean, startingHtml, updatedHtml);
 * 	return updatedHtml;
 * }
 * 
 * &#064;Override
 * protected String resolveLetterBodyField(String startingHtml, X2BaseBean bean) {
 * 	String updatedHtml = super.resolveLetterBodyField(startingHtml, bean);
 * 	emailHelper.updateSection(bean, startingHtml, updatedHtml);
 * 	return updatedHtml;
 * }
 * 
 * &#064;Override
 * protected void teardown() {
 * 	emailHelper.tearDown();
 * 	super.teardown();
 * 
 * 	try {
 * 		emailHelper.sendEmails();
 * 	} catch (Exception e) {
 * 		throw new RuntimeException();
 * 	}
 * }
 * 
 * protected void saveState(UserDataContainer userData) throws X2BaseException {
 * 	super.saveState(userData);
 * 	emailHelper = new QuickLetterEmailHelper(userData, getParameters());
 * }
 * </pre>
 * <p>
 * Additionally you need to either:
 * <ul>
 * <li>Support the input parameters defined here.</li>
 * <li>Manually call methods like {@link #setIncludeContactEmail(boolean)},
 * {@link #setConstantRecipients(List)},
 * {@link #setIncludeStudentEmail(boolean)}, and
 * {@link #setDefaultEmailSubject(String)}.</li>
 * <li>Extend this object and override
 * {@link #getRecipients(X2BaseBean, String)} and
 * {@link #getSubject(X2BaseBean, String)}</li>
 * </ul>
 * <p>
 * If you are using the input parameters to let the user configure how emails
 * are sent, a recommended setup would be:
 * 
 * <pre>
 * &lt;tool-input>
 *   &lt;input name="email-subject" data-type="string" display-type="text" display-name="Email Subject" required="true" default-value="Aspen Notification" /> 
 *   &lt;input name="email-include-student" data-type="boolean" display-type="checkbox" display-name="Email Students" default-value="false" /> 
 *   &lt;input name="email-include-contacts" data-type="boolean" display-type="checkbox" display-name="Email Contacts" default-value="false" /> 
 *   &lt;input name="email-recipients" data-type="string" display-type="text" display-name="Optional Recipient" required="false" default-value="" /> 
 * &lt;/tool-input>
 * </pre>
 */
public class QuickLetterEmailHelper {

	public static final String PARAM_INCLUDE_STUDENT = "email-include-student";
	public static final String PARAM_INCLUDE_CONTACTS = "email-include-contacts";

	/**
	 * This is the required email subject of the email. Note the subject is
	 * constant, but hundreds (thousands?) of emails with the same subject may
	 * be dispatched when this Quick Letter is run.
	 */
	public static final String PARAM_EMAIL_SUBJECT = "email-subject";

	/**
	 * This is an optional semicolon-separated list of email addresses to send
	 * emails to.
	 * <p>
	 * This feature lets you always include yourself or a school administrator
	 * as a recipient of each email.
	 */
	public static final String PARAM_RECIPIENTS = "email-recipients";

	// used for sending emails:
	String ownerOid;
	int ownerType;
	Organization org;
	User user;

	String emailSubject;

	// used for maintaining the active (unsent) email message:
	X2BaseBean activeBean = null;
	List<String> activeSections = new ArrayList<>();

	/**
	 * A map of beans to their HTML email body.
	 * <p>
	 * Since the letters we process are ordered, this map is too.
	 */
	Map<X2BaseBean, String> emailsToSend = new LinkedHashMap<>();

	String defaultSubject;
	boolean includeStudentEmail, includeContactEmail;
	List<String> constantRecipients;

	public QuickLetterEmailHelper(UserDataContainer userData,
			Map<String, Object> parameters) {
		ownerOid = userData.getCurrentOwnerOid();
		ownerType = userData.getCurrentOwnerType();
		org = userData.getOrganization();
		user = userData.getUser();

		setDefaultEmailSubject((String) parameters.get(PARAM_EMAIL_SUBJECT));
		setIncludeStudentEmail(Boolean.TRUE.equals(parameters
				.get(PARAM_INCLUDE_STUDENT)));
		setIncludeContactEmail(Boolean.TRUE.equals(parameters
				.get(PARAM_INCLUDE_CONTACTS)));

		List<String> k = new ArrayList<>();
		String r = (String) parameters.get(PARAM_RECIPIENTS);
		if (r == null)
			r = "";
		for (String x : r.split("\\;")) {
			if (!StringUtils.isEmpty(x)) {
				k.add(x);
			}
		}
		setConstantRecipients(k);
	}

	public void setDefaultEmailSubject(String s) {
		defaultSubject = s;
	}

	public String getDefaultEmailSubject() {
		return defaultSubject;
	}

	/**
	 * Assign the list constant email recipients.
	 * {@link #getConstantRecipients()}
	 */
	public void setConstantRecipients(List<String> list) {
		Objects.requireNonNull(list);
		constantRecipients = new ArrayList<>(list);
	}

	/**
	 * Get the list of constant email recipients. This optional list contains
	 * email addresses that receive every email this helper sends. For example,
	 * you may want to always send a copy of an attendance letter to the
	 * guidance office so they keep a record.
	 * <p>
	 * The default value of this list is derived from the
	 * {@link #PARAM_RECIPIENT} input parameter.
	 */
	public List<String> getConstantRecipients() {
		return new ArrayList<>(constantRecipients);
	}

	/**
	 * Return true if {@link #getRecipients(X2BaseBean, String)} will include
	 * student contacts by default.
	 * <p>
	 * The default value of this boolean is derived from the
	 * {@link #PARAM_INCLUDE_CONTACTS} input parameter.
	 */
	public boolean isIncludeContactEmail() {
		return includeStudentEmail;
	}

	/**
	 * Set whether student contacts receive emails.
	 */
	public void setIncludeContactEmail(boolean b) {
		includeStudentEmail = b;
	}

	/**
	 * Return true if {@link #getRecipients(X2BaseBean, String)} will include
	 * students by default.
	 * <p>
	 * The default value of this boolean is derived from the
	 * {@link #PARAM_INCLUDE_STUDENT} input parameter.
	 */
	public boolean isIncludeStudentEmail() {
		return includeStudentEmail;
	}

	/**
	 * Set whether each student receives emails.
	 */
	public void setIncludeStudentEmail(boolean b) {
		includeStudentEmail = b;
	}

	public void tearDown() {
		finishPreparingActiveEmailIfBeanChanged(null);
	}

	/**
	 * This method should be called from every QuickLetterData method that
	 * starts with <code>resolve</code>.
	 * <p>
	 * In QuickLetterData there's a loop that resembles:
	 * 
	 * <pre>
	 * for(X2BaseBean bean : allBeans) { 
	 *   String body = ""; 
	 *   for(String section : allTemplates) { 
	 *     section = resolveLetterBodyCalculation(section, bean); 
	 *     section = resolveLetterBodyExpression(section, bean); 
	 *     section = resolveLetterBodyField(section, bean);
	 *     body += section; 
	 *   } 
	 *   do something with body 
	 * }
	 * </pre>
	 * 
	 * (The exact order those resolve methods are called in may vary.)
	 * 
	 * So we can override those resolve methods to intercept the HTML we're
	 * composing. Then when the bean changes or when {@link #tearDown()} is
	 * called: we know we're finished with that particular email.
	 *
	 * @param bean
	 *            the bean this HTML relates to.
	 * @param startingHtml
	 *            the HTML before the resolve method has a chance to change it.
	 * @param updatedHtml
	 *            the HTML after the resolve method has changed it.
	 */
	public void updateSection(X2BaseBean bean, String startingHtml,
			String updatedHtml) {
		finishPreparingActiveEmailIfBeanChanged(bean);
		activeBean = bean;
		int i = activeSections.size() - 1;
		if (i >= 0 && activeSections.get(i).equals(startingHtml)) {
			activeSections.set(i, updatedHtml);
		} else {
			activeSections.add(updatedHtml);
		}
	}

	/**
	 * If the argument doesn't match activeBean: then we know we're done with
	 * the current HTML body.
	 */
	private void finishPreparingActiveEmailIfBeanChanged(X2BaseBean bean) {
		if (activeBean != null && !Objects.equals(activeBean, bean)
				&& !activeSections.isEmpty()) {

			StringBuilder messageBody = new StringBuilder();
			messageBody.append("<html>");
			for (String htmlSection : activeSections) {
				messageBody.append(htmlSection);
			}
			messageBody.append("</html>");

			emailsToSend.put(activeBean, messageBody.toString());
		}
		activeSections.clear();
		activeBean = null;
	}

	/**
	 * Send all the emails this helper has composed. This should be called after
	 * {@link #tearDown()}.
	 */
	public void sendEmails() throws Exception {
		WriteEmailManager emailManager = new WriteEmailManager(org, ownerOid,
				ownerType, user);
		if (emailManager.connect()) {
			try {
				// our emails should be ordered, so if an error does occur the
				// user has a remote chance of knowing where we left off.
				Map<X2BaseBean, MessageProperties> messages = new LinkedHashMap<>(
						emailsToSend.size());

				// first prepare all the MessageProperties and make a list of
				// all the problems, so we can throw an exception BEFORE sending
				// any emails.
				// (When it comes to sending emails: we should be
				// all-or-nothing. Don't make the user guess which emails were
				// sent.)
				Collection<String> problems = new HashSet<>();

				for (Entry<X2BaseBean, String> entry : emailsToSend.entrySet()) {
					List<String> recipients = getRecipients(entry.getKey(),
							entry.getValue());
					String subject = getSubject(entry.getKey(),
							entry.getValue());
					if (StringUtils.isEmpty(subject)) {
						problems.add("missing subject");
					}
					for (String recipient : recipients) {
						if (!WriteEmailManager.validateEmailAddress(recipient)) {
							problems.add("invalid recipient: " + recipient
									+ debug(entry.getKey()));
						} else {
							MessageProperties mp = new MessageProperties(
									recipient, null, null, subject,
									entry.getValue(), "text/html");
							messages.put(entry.getKey(), mp);
						}
					}
				}

				if (!problems.isEmpty()) {
					throw new Exception(
							"No emails were sent. Some problems occurred during preprocessing: "
									+ problems);
				}
				for (Entry<X2BaseBean, MessageProperties> entry : messages
						.entrySet()) {
					if (!emailManager.sendMail(entry.getValue())) {
						throw new Exception(
								emailManager.getMessagingException()
										+ debug(entry.getKey()));
					}
					emailsToSend.remove(entry.getKey());
				}
			} finally {
				emailManager.disconnect();
			}
		}
	}

	/**
	 * Return an optional String to append to errors to help users understand
	 * which bean triggered the error.
	 */
	private String debug(X2BaseBean bean) {
		if (bean instanceof Student) {
			return " (for " + ((Student) bean).getNameView() + ")";
		}
		return "";
	}

	/**
	 * Return the subject of the email being sent.
	 */
	protected String getSubject(X2BaseBean bean, String htmlMessageBody) {
		return defaultSubject;
	}

	/**
	 * Return the email recipients.
	 */
	protected List<String> getRecipients(X2BaseBean bean, String htmlMessageBody) {
		List<String> returnValue = new ArrayList<>();

		List<Person> persons = new ArrayList<>();
		if (bean instanceof Student && isIncludeStudentEmail()) {
			Student student = (Student) bean;
			persons.add(student.getPerson());
		}

		if (bean instanceof Student && isIncludeContactEmail()) {
			Student student = (Student) bean;
			for (StudentContact contact : student.getContacts()) {
				if (contact.getReceiveEmailIndicator())
					persons.add(contact.getPerson());
			}
		}

		for (Person person : persons) {
			String email01 = person.getEmail01();

			// should we consult other email fields? (email02, emailGoogle)
			if (!StringUtils.isEmpty(email01))
				returnValue.add(email01);
		}

		returnValue.addAll(getConstantRecipients());
		return returnValue;
	}
}
