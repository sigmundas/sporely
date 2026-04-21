-- Create user_blocks table
CREATE TABLE IF NOT EXISTS public.user_blocks (
    blocker_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    blocked_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (blocker_id, blocked_id)
);

-- Enable Row Level Security
ALTER TABLE public.user_blocks ENABLE ROW LEVEL SECURITY;

-- RLS Policies
-- Users can see blocks involving them (required to filter feed visibility appropriately)
CREATE POLICY "Users can view their own blocks" 
    ON public.user_blocks FOR SELECT 
    USING (auth.uid() = blocker_id OR auth.uid() = blocked_id);

-- Users can block other users
CREATE POLICY "Users can insert their own blocks" 
    ON public.user_blocks FOR INSERT 
    WITH CHECK (auth.uid() = blocker_id);

-- Users can unblock other users
CREATE POLICY "Users can delete their own blocks" 
    ON public.user_blocks FOR DELETE 
    USING (auth.uid() = blocker_id);