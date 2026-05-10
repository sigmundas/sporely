-- Ensure the avatars bucket is public
UPDATE storage.buckets
SET public = true
WHERE id = 'avatars';
