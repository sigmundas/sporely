-- ==============================================================================
-- Enforcement for Banned Users (Write Access)
-- 
-- While this can be done via RLS, RLS policies combine with OR.
-- To enforce a global ban safely without having to drop and recreate every single
-- existing policy (which requires knowing their exact names), a trigger is
-- bulletproof. It ensures banned users cannot post regardless of RLS state.
-- ==============================================================================

CREATE OR REPLACE FUNCTION public.check_user_banned()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.profiles WHERE id = auth.uid() AND is_banned = true) THEN
        RAISE EXCEPTION 'User is banned and cannot perform this action.';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS enforce_ban_observations ON public.observations;
CREATE TRIGGER enforce_ban_observations
    BEFORE INSERT OR UPDATE ON public.observations
    FOR EACH ROW EXECUTE FUNCTION public.check_user_banned();

DROP TRIGGER IF EXISTS enforce_ban_observation_images ON public.observation_images;
CREATE TRIGGER enforce_ban_observation_images
    BEFORE INSERT OR UPDATE ON public.observation_images
    FOR EACH ROW EXECUTE FUNCTION public.check_user_banned();

DROP TRIGGER IF EXISTS enforce_ban_comments ON public.comments;
CREATE TRIGGER enforce_ban_comments
    BEFORE INSERT OR UPDATE ON public.comments
    FOR EACH ROW EXECUTE FUNCTION public.check_user_banned();