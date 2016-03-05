{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Language.Haskell.TH.TypeHash
    ( thTypeHash
    , thTypeHashForNames
    ) where

import Control.Monad ((<=<))
import Data.Generics (listify)
import Data.Hashable (Hashable(hash))
import Data.List (sortBy)
import Data.Ord (comparing)
import Language.Haskell.TH
import Language.Haskell.TH.Instances ()
import Language.Haskell.TH.ReifyMany (reifyMany, reifyManyWithoutInstances)
import Language.Haskell.TH.Syntax (lift)

-- TODO: move into th-orphans, particularly if this library gets released.
$(reifyManyWithoutInstances ''Hashable [''Info, ''Loc] (const True) >>=
  mapM (\name -> return (InstanceD [] (AppT (ConT ''Hashable) (ConT name)) [])))

-- TODO: move into th-reify-many, particularly if this library gets released.
reifyManyTyDecls :: ((Name, Info) -> Q (Bool, [Name]))
                 -> [Name]
                 -> Q [(Name, Info)]
reifyManyTyDecls f = reifyMany go
  where
    go x@(_, TyConI{}) = f x
    go x@(_, FamilyI{}) = f x
    go x@(_, PrimTyConI{}) = f x
    go x@(_, DataConI{}) = f x
    go (_, ClassI{}) = return (False, [])
    go (_, ClassOpI{}) = return (False, [])
    go (_, VarI{}) = return (False, [])
    go (_, TyVarI{}) = return (False, [])

-- | At compiletime, this yields a hash of the specified datatypes.
-- Not only does this cover the datatypes themselves, but also all
-- transitive dependencies.
--
-- The resulting expression is a literal of type 'Int'.
thTypeHashForNames :: [Name] -> Q Exp
thTypeHashForNames = lift . hash <=< getTypeInfosRecursively

-- | At compiletime, this yields a hash of the specified 'Type',
-- including the definition of things it references (transitively).
--
-- The resulting expression is a literal of type 'Int'.
thTypeHash :: Type -> Q Exp
thTypeHash ty = do
    infos <- getTypeInfosRecursively (listify (\_ -> True) ty)
    lift (hash (ty, infos))

getTypeInfosRecursively :: [Name] -> Q [(Name, Info)]
getTypeInfosRecursively names = do
    allInfos <- reifyManyTyDecls (\(_, info) -> return (True, listify (\_ -> True) info)) names
    -- Sorting step probably unnecessary because this should be
    -- deterministic, but hey why not.
    return (sortBy (comparing fst) allInfos)
