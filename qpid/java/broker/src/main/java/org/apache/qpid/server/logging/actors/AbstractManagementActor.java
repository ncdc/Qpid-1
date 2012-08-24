package org.apache.qpid.server.logging.actors;

import java.security.AccessController;

import javax.security.auth.Subject;

import org.apache.qpid.server.logging.RootMessageLogger;
import org.apache.qpid.server.security.auth.AuthenticatedPrincipal;

public abstract class AbstractManagementActor extends AbstractActor
{
    /**
     * Holds the principal name to display when principal subject is not available.
     * <p>
     * This is useful for cases when users invoke JMX operation over JConsole
     * attached to the local JVM.
     */
    protected static final String UNKNOWN_PRINCIPAL = "N/A";

    /** used when the principal name cannot be discovered from the Subject */
    private final String _fallbackPrincipalName;

    public AbstractManagementActor(RootMessageLogger rootLogger, String fallbackPrincipalName)
    {
        super(rootLogger);
        _fallbackPrincipalName = fallbackPrincipalName;
    }

    /**
     * Returns current {@link AuthenticatedPrincipal} name or {@link #_fallbackPrincipalName}
     * if it can't be found.
     */
    protected String getPrincipalName()
    {
        String identity = _fallbackPrincipalName;

        final Subject subject = Subject.getSubject(AccessController.getContext());
        if (subject != null)
        {
            AuthenticatedPrincipal authenticatedPrincipal = AuthenticatedPrincipal.getOptionalAuthenticatedPrincipalFromSubject(subject);
            if(authenticatedPrincipal != null)
            {
                identity = authenticatedPrincipal.getName();
            }
        }
        return identity;
    }
}