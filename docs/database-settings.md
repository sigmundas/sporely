# Database Settings

The **Database Settings** dialog controls where Sporely stores its databases and image folder, and it also lets you manage the built-in microscopy tag lists:

- contrast methods
- mount media
- stains
- sample types
- measure categories

The mount and stain lists are meant as practical lab tags, not as a formal ontology. You can disable built-in items or add your own custom tags.

Hovering built-in tags in the dialog shows a short hint in the status area and as a tooltip. The longer notes below are the reference version of those hints.

## Mount Media

### Water

Use for neutral observation and reference measurements with minimal chemical disturbance.

- Chemistry: aqueous mount with no strong clearing or swelling effect
- Typical use: baseline spore measurements and quick fresh mounts
- Main effect: preserves general shape well, but contrast is often modest

### KOH

Potassium hydroxide is a classic alkaline clearing reagent.

- Chemistry: strong base
- Typical use: clearing tissues, reviving dried material, pigment reactions
- Main effect: clears cytoplasm and can alter pigmentation; may also change dimensions in some tissues

### NH3

Ammonia is another alkaline reagent used mainly for reactions and contrast shifts.

- Chemistry: weak base
- Typical use: pigment reactions and supplementary observation
- Main effect: can change pigment appearance and improve visibility of some structures

### Glycerine

Glycerine is useful for slower-drying, more stable mounts.

- Chemistry: hygroscopic polyol
- Typical use: semi-permanent mounts and prolonged observation
- Main effect: slows evaporation and can keep tissues pliable

### L4

L4 is an alkaline glycerol mount associated with Clémençon’s microscopy work.

- Chemistry: alkaline aqueous-glycerol mount with KOH, NaCl, glycerol, and a small amount of wetting agent
- Typical use: clearer, more stable mounts for fungal tissues, especially when you want some clearing together with reduced drying
- Main effect: combines mild clearing with a viscous mount that keeps tissues from drying too fast
- Note: Clémençon (1972), *Zeitschrift für Pilzkunde* 38: 49-53

#### L4 (100 mL) with Photo-Flo

Components:

- Distilled water: about 84 mL
- KOH pellets: 0.72 g
- NaCl: 0.76 g
- Glycerol (pure): 20 g, about 16 mL
- Photo-Flo stock: 0.2 to 0.5 mL

Mixing order:

1. Add about 70 mL distilled water.
2. Dissolve 0.76 g NaCl.
3. Dissolve 0.72 g KOH.
4. Add 20 g glycerol, about 16 mL, and mix.
5. Add Photo-Flo, 0.2 to 0.5 mL.
6. Top up with distilled water to 100 mL total.
7. Mix gently and avoid foaming.

Photo-Flo acts as a wetting agent and helps the mount spread more evenly. Tween or another mild surfactant can be used instead if Photo-Flo is not available.

Compared with plain KOH or water mounts, L4 is often useful when you want a preparation that clears tissue but still behaves well during a longer session at the microscope.

## Stains

### Melzer

Melzer is primarily a reaction reagent rather than a simple contrast stain.

- Chemistry: iodine-based reagent
- Typical use: testing amyloid and dextrinoid reactions
- Reacts with: starch-like wall chemistry in spores, asci, and other structures
- Main effect: blue-black amyloid or reddish-brown dextrinoid reactions when present

### Congo Red

A strong red counterstain for fungal walls.

- Chemistry: diazo dye
- Typical use: increasing wall contrast in mounts, often together with alkaline reagents
- Reacts with: cell-wall material by adsorption rather than a highly specific diagnostic reaction
- Main effect: outlines walls and improves visibility of shape and ornamentation

### Cotton Blue

Classic blue wall stain, often discussed in relation to cyanophily.

- Chemistry: aniline dye
- Typical use: testing cyanophilous reactions and improving wall visibility
- Reacts with: chitin-rich walls and cyanophilous structures
- Main effect: blue staining of walls; strength depends on tissue chemistry and reagent system

### Lactofuchsin

A strong general-purpose fungal stain.

- Chemistry: fuchsin dye in a lactic-acid-based system
- Typical use: general tissue contrast in mounts
- Reacts with: fungal walls and contents broadly
- Main effect: high contrast, especially useful for thin-walled structures and tissue details

### Cresyl Blue

A basic dye used for contrast and metachromatic effects.

- Chemistry: basic oxazine dye
- Typical use: bringing out subtle wall and content differences in fungal tissues
- Reacts with: acidic cell components and some wall materials
- Main effect: enhanced contrast, sometimes with metachromatic color shifts

### Trypan Blue

The standard chemical name is **Trypan Blue**. If you have notes elsewhere using "Tryptan Blue", they refer to the same dye.

- Chemistry: diazo dye
- Typical use: blue counterstain for tissues and damaged cells
- Reacts with: tissue by adsorption; commonly used for contrast rather than a sharply diagnostic wall reaction
- Main effect: blue staining of tissues, often useful when structures are otherwise hard to separate from the background

### Chlorazol Black E

A dark wall stain with strong affinity for chitinous structures.

- Chemistry: direct azo dye
- Typical use: staining fungal walls, septa, and fine structures with strong contrast
- Reacts with: chitin-rich cell walls
- Main effect: dark blue-black to black wall staining, often excellent for outlines and small details

## Practical Note

Mounts and stains can change apparent size, wall visibility, contents, and reaction colors. If you are building a reference dataset, it is worth recording both the **mount medium** and the **stain** so measurements from different preparations do not get mixed without context.

## See also

- [Microscopy workflow](./microscopy-workflow.md)
- [Database structure](./database-structure.md)
