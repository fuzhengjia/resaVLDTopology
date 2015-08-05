package topology;

import tool.Serializable;

/**
 * Created by Intern04 on 19/8/2014.
 */
public class LogoTemplateUpdate {
    //Fields("hostPatchIdentifier", "detectedLogoRect", "parentIdentifier"));
    public final Serializable.PatchIdentifier hostPatchIdentifier;
    public final Serializable.Rect detectedLogoRect;
    public final Serializable.PatchIdentifier parentIdentifier;
    public LogoTemplateUpdate(Serializable.PatchIdentifier hostPatchIdentifier,
                              Serializable.Rect detectedLogoRect,
                              Serializable.PatchIdentifier parentIdentifier)
    {
        this.hostPatchIdentifier = hostPatchIdentifier;
        this.detectedLogoRect = detectedLogoRect;
        this.parentIdentifier = parentIdentifier;
    }
}
